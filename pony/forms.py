import re, threading, os.path, copy, cPickle

from operator import attrgetter
from itertools import count, izip

from pony import auth
from pony.utils import decorator, converters, ValidationError
from pony.templating import Html, StrHtml, htmljoin, htmltag
from pony.web import get_request, Http400BadRequest, HttpRedirect

class FormNotProcessed(Exception): pass

class FormMeta(type):
    def __new__(meta, name, bases, dict):
        if 'Form' in globals():
            for value in dict.values():
                if isinstance(value, HtmlField): raise TypeError(
                    'You cannot place fields inside form class directly. Use __init__ method instead')
        init = dict.get('__init__')
        if init is not None: dict['__init__'] = _form_init_decorator(init)
        return super(FormMeta, meta).__new__(meta, name, bases, dict)

@decorator
def _form_init_decorator(__init__):
    def new_init(form, *args, **keyargs):
        try: init_counter = form._init_counter
        except AttributeError:
            if form.__class__ is not Form: Form.__init__.original_func(form)
            object.__setattr__(form, '_init_args', cPickle.dumps((args, keyargs), 2))
            object.__setattr__(form, '_init_counter', 1)
        else: object.__setattr__(form, '_init_counter', init_counter+1)
        try: __init__(form, *args, **keyargs)
        finally:
            init_counter = form._init_counter
            object.__setattr__(form, '_init_counter', init_counter-1)
    return new_init

http_303_incompatible_browsers = []

class Form(object):
    __metaclass__ = FormMeta
    def __setattr__(self, name, x):
        prev = getattr(self, name, None)
        if not isinstance(x, HtmlField):
            if isinstance(prev, HtmlField): self.__delattr__(name)
            object.__setattr__(self, name, x)
            return
        if hasattr(self, name):
            if not isinstance(prev, HtmlField):
                raise TypeError('Invalid form field name: %s' % name)
            try:
                # try..except is necessary because __init__ can be called twice
                if   isinstance(prev, Hidden): self.hidden_fields.remove(prev)
                elif isinstance(prev, Submit): self.submit_fields.remove(prev)
                else: self.fields.remove(prev)
            except ValueError: pass
        if   isinstance(x, Hidden): self.hidden_fields.append(x)
        elif isinstance(x, Submit): self.submit_fields.append(x)
        else: self.fields.append(x)
        object.__setattr__(self, name, x)
        x._init_(self, name, name.replace('_', ' ').capitalize())
    def __delattr__(self, name):
        x = getattr(self, name)
        if isinstance(x, HtmlField):
            try:
                # try..except is necessary because __init__ can be called twice
                if   isinstance(x, Hidden): self.hidden_fields.remove(x)
                elif isinstance(x, Submit): self.submit_fields.remove(x)
                else: self.fields.remove(x)
            except ValueError: pass
        object.__delattr__(self, name)
    def __init__(self, method='POST', secure=None,
                 prevent_resubmit=False, buttons_align=None, **attrs):
        # Note for subclassers: __init__ can be caled twice!
        object.__setattr__(self, '_pickle_entire_form', False)
        object.__setattr__(self, '_cleared', False)
        object.__setattr__(self, '_validated', False)
        object.__setattr__(self, '_error_text', None)
        object.__setattr__(self, '_request', get_request())
        object.__setattr__(self, 'attrs', dict((name.lower(), str(value))
                                               for name, value in attrs.iteritems()))
        if 'name' not in attrs: self.attrs['name'] = self.__class__.__name__
        object.__setattr__(self, 'fields', [])
        object.__setattr__(self, 'hidden_fields', [])
        object.__setattr__(self, 'submit_fields', [])
        object.__setattr__(self, '_secure', False)
        self._set_method(method)
        self._set_secure(secure)
        object.__setattr__(self, 'prevent_resubmit', prevent_resubmit)
        object.__setattr__(self, 'buttons_align', buttons_align)
        self._f = Hidden(self.attrs.get('name', ''))
    def __getstate__(self):
        state = self._init_args
        if self._pickle_entire_form or state is None:
            state = self.__dict__.copy()
            for attr in ('_pickle_entire_form', '_init_args', '_init_counter',
                         '_cleared', '_validated', '_error_text', '_request', 'is_submitted'):
                state.pop(attr, None)
        return state
    def __setstate__(self, state):
        if isinstance(state, str):
            args, keyargs = cPickle.loads(state)
            self.__init__(*args, **keyargs)
        elif isinstance(state, dict):
            state['_pickle_entire_form'] = True
            state['_init_args'] = None
            state['_init_counter'] = 0
            state['_cleared'] = state['_validated'] = False
            state['_error_text'] = None
            state['_request'] = get_request()
            self.__dict__.update(state)
            self._update_status()
        else: assert False
    def _handle_request_(self):
        request = get_request()
        if not self.is_valid:
            request.form_processed = False
            return
        try: without_redirect = self.on_submit()
        except FormNotProcessed: request.form_processed = False
        else:
            if request.form_processed is None: request.form_processed = True
            if without_redirect: return
            user_agent = request.environ.get('HTTP_USER_AGENT', '')
            for browser in http_303_incompatible_browsers:
                if browser in user_agent: raise HttpRedirect('.', status='302 Found')
            raise HttpRedirect(request.full_url, status='303 See Other')
    def clear(self):
        object.__setattr__(self, '_cleared', True)
        object.__setattr__(self, 'is_submitted', False)
    def _set_method(self, method):
        method = method.upper()
        if method not in ('GET', 'POST'): raise TypeError(
            'Invalid form method: %s (must be GET or POST)' % method)
        if method == 'GET' and self._secure:
            raise TypeError('GET form cannot be secure')
        object.__setattr__(self, '_method', method)
        self._update_status()
    method = property(attrgetter('_method'), _set_method)
    def _set_secure(self, secure):
        if self._method == 'GET':
            if secure: raise TypeError('GET form cannot be secure')
            object.__setattr__(self, '_secure', False)
        elif self.method == 'POST': object.__setattr__(self, '_secure', secure or secure is None)
        else: assert False
        if self._secure: self._t = Ticket()
        elif hasattr(self, '_t'): del self._t
        self._update_status()
    secure = property(attrgetter('_secure'), _set_secure)
    def _update_status(self):
        object.__setattr__(self, 'is_submitted', False)
        request = self._request
        if self._cleared or request.form_processed: return
        if request.form_processed is not None \
           and request.submitted_form != self.attrs.get('name'): return
        if self.method == 'POST' and request.method != 'POST': return
        object.__setattr__(self, 'is_submitted', True)
    @property
    def is_valid(self):
        if not self.is_submitted: return False
        self._validate()
        if self._error_text: return False
        for f in self.hidden_fields:
            if not f.is_valid: return False
        for f in self.fields:
            if not f.is_valid: return False
        if self._secure and not self._request.ticket:
            return self._request.ticket  # may be False or None
        return True
    def _validate(self):
        if self._validated: return
        object.__setattr__(self, '_validated', True)
        self.validate()
    def validate(self):
        pass
    def _get_error_text(self):
        if not self.is_submitted: return None
        if self._cleared or self._request.form_processed: return None
        if self._error_text is not None: return self._error_text
        self._validate()
        for f in self.fields:
            if f.error_text: return 'Some fields below contains errors'
        if self.is_valid is None:
            return 'The form has already been submitted'
    def _set_error_text(self, text):
        object.__setattr__(self, '_error_text', text)
    error_text = property(_get_error_text, _set_error_text)
    @property
    def error(self):
        error_text = self.error_text
        if not error_text: return ''
        return Html('<div class="error">%s</div>' % error_text)
    @property
    def tag(self):
        attrs = self.attrs
        for f in self.fields:
            if isinstance(f, File):
                attrs['enctype'] = 'multipart/form-data'
                break
        error_class = self.error_text and 'has-error' or ''
        return htmltag('form', attrs, method=self.method, accept_charset='UTF-8',
                       _class=error_class)
    @property
    def header(self):
        result = [ self.tag ]
        for f in self.hidden_fields: result.append(f.html)
        for f in self.fields:
            hidden = f.hidden
            if hidden: result.append(hidden)
        return Html('\n') + Html('\n').join(result)
    @property
    def table(self):
        result = []
        for f in self.fields:
            classes = f.__class__.__name__.lower() + '-field'
            if f.error_text: classes += ' has-error'
            result.extend((Html('\n<tr class="%s">\n<th>' % classes),
                           f.label, Html('</th>\n<td>'), f.tag))
            error = f.error
            if error: result.append(error)
            result.append(Html('</td></tr>'))
        return htmljoin(result)
    @property
    def buttons(self):
        result = [ htmltag('div', _class='buttons', align=self.buttons_align) ]
        buttons = [ f.html for f in self.submit_fields ]
        result.extend(buttons or [ htmltag('input', type='submit') ])
        result.append(Html('\n</div>'))
        return htmljoin(result)
    def __str__(self):
        return StrHtml(unicode(self).encode('ascii', 'xmlcharrefreplace'))
    def __unicode__(self):
        if self.buttons_align is None:
              buttons = Html('\n<tr><td>&nbsp;</td><td>%s</td></tr>') % self.buttons
        else: buttons = Html('\n<tr><td colspan="2">%s</td></tr>') % self.buttons
        return htmljoin([ self.header, self.error,
                          Html('\n<table>'),
                          self.table,
                          buttons,
                          Html('\n</table></form>\n')])
    html = property(__unicode__)
Form.ValidationError = ValidationError
Form.NotProcessed = FormNotProcessed
Form.DoNotDoRedirect = DoNotDoRedirect = True

class HtmlField(object):
    def __init__(self, value=None):
        self.attrs = {}
        self.form = self.name = None
        self.initial_value = value
        self._label = None
    def _init_(self, form, name, label):
        object.__setattr__(form, '_validated', False)
        self.form = form
        self.name = name
        if self._label is None: self._label = label
    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('_initial_value', None)
        state.pop('_new_value', None)
        return state
    def __setstate__(self, state):
        self.__dict__.update(state)
        self._initial_value = None
    @property
    def is_submitted(self):
        form = self.form
        if form is None or not form.is_submitted: return False
        fields = form._request.fields
        if fields.getfirst(self.name) is not None: return True
        return fields.getfirst('.'+self.name) is not None
    @property
    def is_valid(self):
        return self.is_submitted
    def _get_value(self):
        try: return self._new_value
        except AttributeError:
            if not self.is_submitted: return self.initial_value
            value = self.form._request.fields.getfirst(self.name)
            if value is None: return None
            try: return unicode(value, 'utf8')
            except UnicodeDecodeError: raise Http400BadRequest
    def _set_value(self, value):
        form = self.form
        if form is None or form._init_counter: self.initial_value = value
        else:
            self._new_value = value
            object.__setattr__(form, '_validated', False)
    value = property(_get_value, _set_value)
    html_value = property(_get_value)
    def __unicode__(self):
        value = self.html_value
        if value is None: value = ''
        return htmltag('input', self.attrs, name=self.name, value=value, type=self.HTML_TYPE)
    tag = html = property(__unicode__)
    def __str__(self):
        return StrHtml(unicode(self).encode('ascii', 'xmlcharrefreplace'))
    def __repr__(self):
        return '<%s: %s>' % (self.name or '?', self.__class__.__name__)

class Hidden(HtmlField):
    HTML_TYPE = 'hidden'

class Ticket(Hidden):
    def _get_value(self):
        form = self.form
        if form is not None and hasattr(form, 'on_submit'): payload = cPickle.dumps(form, 2)
        else: payload = None
        return auth.get_ticket(payload, form.prevent_resubmit)
    def _set_value(self, value):
        raise TypeError('Cannot set value for tickets')
    value = property(_get_value, _set_value)
    html_value = property(_get_value)

class Submit(HtmlField):
    HTML_TYPE = 'submit'
    def _init_(self, form, name, label):
        HtmlField._init_(self, form, name, label)
        if self.initial_value is None: self.initial_value = label

class Reset(Submit):
    HTML_TYPE = 'reset'

class BaseWidget(HtmlField):
    def __init__(self, label=None, required=None, value=None, **attrs):
        if 'type' in attrs: raise TypeError('You can set type only for Text fields')
        if 'regex' in attrs: raise TypeError('You can set regex only for Text fields')
        HtmlField.__init__(self, value)
        if 'id' not in attrs:
            request = get_request()
            attrs['id'] = request.id_counter.next()
        self.attrs = attrs
        self.required = required
        self._error_text = None
        self._auto_error_text = None
        self._set_label(label)
    def __getstate__(self):
        dict = HtmlField.__getstate__(self)
        dict.pop('_label', None)
        dict.pop('_error_text', None)
        dict.pop('_auto_error_text', None)
        return dict
    def __setstate__(self, state):
        HtmlField.__setstate__(self, state)
        self.label = None
        self._error_text = None
        self._auto_error_text = None
    @property
    def is_valid(self):
        return self.is_submitted and not self.error_text
    def _get_error_text(self):
        form = self.form
        if form is None or form._cleared or form._request.form_processed: return None
        if self._error_text: return self._error_text
        if self.is_submitted: return self._check_error()
        return None
    def _set_error_text(self, text):
        self._error_text = text
    error_text = property(_get_error_text, _set_error_text)
    def _check_error(self):
        value = self.value
        if self._auto_error_text: return self._auto_error_text
        if self.required and not value: return 'This field is required'
    @property
    def error(self):
        error_text = self.error_text
        if not error_text: return ''
        return Html('<div class="error">%s</div>') % error_text
    def _get_label(self, colon=True, required=True):
        if not self._label: return ''
        if isinstance(self._label, Html): return self._label
        if not (required and self.required): required_html = ''
        else: required_html = Html('<sup class="required">*</sup>')
        colon_html = colon and Html('<span class="colon">:</span>') or ''
        return Html('<label for="%s">%s%s%s</label>') % (
            self.attrs['id'], self._label, required_html, colon_html)
    def _set_label(self, label):
        self._label = label
    label = property(_get_label, _set_label)
    def __unicode__(self):
        return htmljoin((self._label, self.tag, self.error))
    html = property(__unicode__)
    @property
    def hidden(self):
        if not self.attrs.get('disabled'): return ''
        value = self.html_value
        if value is None: value = ''
        return htmltag('input', type='hidden', name=self.name, value=value)

class File(BaseWidget):
    HTML_TYPE = 'file'
    def _get_value(self):
        if not self.is_submitted: return None
        fields = self.form._request.fields
        try: filename = fields[self.name].filename
        except: return None
        if not filename: return None
        return fields[self.name].file
    def _set_value(self, value):
        raise TypeError('This property cannot be set')
    value = property(_get_value, _set_value)
    @property
    def filename(self):
        if not self.is_submitted: return None
        fields = self.form._request.fields
        try: filename = fields[self.name].filename
        except: return None
        if not filename: return None
        return os.path.basename(filename)
    @property
    def tag(self):
        return htmltag('input', self.attrs, name=self.name, type=self.HTML_TYPE)

class Password(BaseWidget):
    HTML_TYPE = 'password'

class Text(BaseWidget):
    HTML_TYPE = 'text'
    def __init__(self, label=None, required=None, value=None, type=None, regex=None, **attrs):
        BaseWidget.__init__(self, label, required, value, **attrs)
        if isinstance(type, basestring) and type not in converters:
            raise TypeError('Unknown field type value: %r' % type)
        elif isinstance(type, tuple):
            if len(type) == 2: type += (None,)
            elif len(type) != 3:
                raise TypeError('Type tuple length must be 2 or 3. Got: %d' % len(type))
        self.type = type
        if isinstance(regex, basestring): regex = re.compile(regex, re.UNICODE)
        self.regex = regex
    def _get_value(self):
        value = BaseWidget._get_value(self)
        if not value: return None
        if self.regex is not None:
            match = self.regex.match(value)
            if match is None:
                self._auto_error_text = 'Invalid data'
                return None
        type = self.type
        if type is None or not isinstance(value, unicode): return value
        if isinstance(type, tuple): str2py, py2str, err_msg = type
        else: str2py, py2str, err_msg = converters.get(type, (self.type, unicode, None))
        try: return str2py(value)
        except ValidationError, e: err_msg = e.err_msg
        except: pass
        self._auto_error_text = err_msg or 'Invalid data'
        return None
    value = property(_get_value, BaseWidget._set_value)
    @property
    def html_value(self):
        value = BaseWidget._get_value(self)
        type = self.type
        if value is None or type is None or isinstance(value, unicode): return value
        if isinstance(type, tuple): str2py, py2str, err_msg = type
        else: str2py, py2str, err_msg = converters.get(type, (self.type, unicode, None))
        return py2str(value)

class StaticText(BaseWidget):
    def __unicode__(self):
        return Html('<strong>%s</strong>') % self.value
    html = tag = property(__unicode__)
    @property
    def hidden(self):
        return htmltag('input', type='hidden', name=self.name, value=self.value)

class TextArea(BaseWidget):
    @property
    def tag(self):
        result = [ htmltag('textarea', self.attrs, name=self.name) ]
        if self.value is not None: result.append(self.value)
        result.append(Html('</textarea>'))
        return htmljoin(result)

class Checkbox(BaseWidget):
    HTML_TYPE = 'checkbox'
    def _get_value(self):
        return bool(BaseWidget._get_value(self))
    def _set_value(self, value):
        BaseWidget._set_value(self, bool(value))
    value = property(_get_value, _set_value)
    @property
    def tag(self):
        result = []
        result.append(htmltag('input', self.attrs, name=self.name,
                              value='yes', checked=bool(self.value),
                              type = self.HTML_TYPE))
        return htmljoin(result)
    @property
    def hidden(self):
        return htmltag('input', name='.'+self.name, type='hidden', value='')

class Select(BaseWidget):
    def __init__(self, label=None, required=False, value=None, options=[], **attrs):
        BaseWidget.__init__(self, label, required, **attrs)
        self._set_options(options)
        self.value = value
        size = attrs.get('size')
        if size is not None: pass
        elif not isinstance(self, MultiSelect): self.attrs['size'] = 1
        else: self.attrs['size'] = min(len(self.options), 5)
    def _set_options(self, options):
        self.keys = {}
        self.values = {}
        options = list(options)
        for i, option in enumerate(options):
            if isinstance(option, tuple):
                if len(option) == 3: value, description, key = option
                elif len(option) == 2:
                    value, description = option
                    key = unicode(value)
                else: raise TypeError('Invalid option: %r' % option)
                description = unicode(description)
            else:
                value = option
                key = description = unicode(value)
            option = value, description, key
            x = self.keys.setdefault(key, option)
            if x is not option: raise TypeError('Duplicate option key: %s' % key)
            x = self.values.setdefault(value, option)
            if x is not option: raise TypeError('Duplicate option value: %s' % value)
            options[i] = option
        self._options = tuple(options)
        form = self.form
        if form is not None: object.__setattr__(form, '_validated', False)
    options = property(attrgetter('_options'), _set_options)
    def _get_value(self): # for Select and RadioGroup
        try: return self._new_value
        except AttributeError:
            if not self.is_submitted: return self.initial_value
            key = self.form._request.fields.getfirst(self.name)
            if key is None: return None
            try: key = unicode(key, 'utf8')
            except UnicodeDecodeError: raise Http400BadRequest
            option = self.keys.get(key)
            if option is None: return None
            return option[0]
    def _set_value(self, value): # for Select and RadioGroup
        if value is not None and value not in self.values:
            raise TypeError('Invalid widget value: %r' % value)
        form = self.form
        if form is None or form._init_counter: self.initial_value = value
        else:
            self._new_value = value
            object.__setattr__(form, '_validated', False)
    value = property(_get_value, _set_value)
    @property
    def tag(self): # for Select and MultiSelect
        result = [ htmltag('select', self.attrs, name=self.name, multiple=isinstance(self, MultiSelect)) ]
        value = self.value
        if isinstance(self, MultiSelect): selection = value
        elif value is None: selection = set()
        else: selection = set((value,))
        for value, description, key in self.options:
            if key == description: key = None
            result.append(htmltag('option', selected=(value in selection), value=key))
            result.append(description)
            result.append(Html('</option>'))
        result.append(Html('</select>'))
        return htmljoin(result)
    @property
    def hidden(self):
        if self.__class__ == Select and str(self.attrs.get('size', '')) == '1': return ''
        return htmltag('input', name='.'+self.name, type='hidden', value='')

class RadioGroup(Select):
    @property
    def tag(self):
        result = [ htmltag('div', self.attrs, _class='radiobuttons') ]
        selected = self.value
        for value, description, key in self.options:
            result.append(Html('<div class="radiobutton">'))
            result.append(htmltag('input', type='radio', name=self.name,
                                  value=key, checked=(value==selected)))
            result.append(Html('<span class="value">%s</span></div>') % description)
        result.append(Html('</div>'))
        result.append(htmltag('input', name='.'+self.name, type='hidden', value=''))
        return htmljoin(result)

class MultiSelect(Select):
    def _get_value(self):
        try: return self._new_value
        except AttributeError:
            if not self.is_submitted: return self.initial_value.copy()
            keys = self.form._request.fields.getlist(self.name)
            result = set()
            for key in keys:
                try: key = unicode(key, 'utf8')
                except UnicodeDecodeError: raise Http400BadRequest
                option = self.keys.get(key)
                if option is not None: result.add(option[0])
            return result
    def _set_value(self, value):
        if value is None: values = set()
        elif isinstance(value, basestring): values = set((value,))
        elif hasattr(value, '__iter__'): values = set(value)
        else: values = set((value,))
        for value in values:
            if value not in self.values: raise TypeError('Invalid widget value: %r' % value)
        form = self.form
        if form is None or form._init_counter: self.initial_value = values
        else:
            self._new_value = values
            object.__setattr__(form, '_validated', False)
    value = property(_get_value, _set_value)

class CheckboxGroup(MultiSelect):
    @property
    def tag(self):
        result = [ htmltag('div', self.attrs, _class='checkboxes') ]
        selection = self.value
        for value, description, key in self.options:
            result.append(Html('<div class="checkbox">'))
            result.append(htmltag('input', name=self.name, type='checkbox',
                                  value=value, checked=(value in selection)))
            result.append(Html('<span class="value">%s</span></div>') % description)
        result.append(Html('</div>'))
        result.append(htmltag('input', name='.'+self.name, type='hidden', value=''))
        return htmljoin(result)
    
class Composite(BaseWidget):
    def __init__(self, label=None, required=None, show_headers=True, **attrs):
        BaseWidget.__init__(self, label, required, **attrs)
        self.show_headers = show_headers
        self.hidden_fields = []
        self.fields = []
    def __setattr__(self, name, x):
        prev = getattr(self, name, None)
        if not isinstance(x, HtmlField):
            if isinstance(prev, HtmlField): self.__delattr__(name)
            object.__setattr__(self, name, x)
            return
        if self.form is None: raise TypeError('You must first assign the Composite object to the form')
        if hasattr(self, name):
            if not isinstance(prev, HtmlField): raise TypeError('Invalid composite item name: %s' % name)
            elif isinstance(prev, Hidden): self.hidden_fields.remove(prev)
            else: self.fields.remove(prev)
        if self.required is not None and x.required is None: x.required = self.required
        if isinstance(x, Hidden): self.hidden_fields.append(x)
        else: self.fields.append(x)
        object.__setattr__(self, name, x)
        field_name = '%s.%s' % (self.name, name)
        field_label = name.replace('_', ' ').capitalize()
        x._init_(self.form, field_name, field_label)
    def __delattr__(self, name):
        x = getattr(self, name)
        if isinstance(x, Hidden): self.hidden_fields.remove(x)
        elif isinstance(x, HtmlField): self.fields.remove(x)
        object.__delattr__(self, name)
    @property
    def is_submitted(self):
        form = self.form
        if form is None or not form.is_submitted: return False
        for field in self.fields:
            if field.is_submitted: return True
        return False
    def _get_error_text(self):
        form = self.form
        if form is None or not form.is_submitted: return None
        if form._cleared or form._request.form_processed: return None
        if self._error_text: return self._error_text
        result = []
        for field in self.fields:
            if isinstance(field, Submit): continue
            error_text = field.error_text
            if not error_text: continue
            result.append('%s: %s' % (field._label, error_text))
        result = '\n'.join(result)
        if result.isspace(): return None
        return result
    error_text = property(_get_error_text, BaseWidget._set_error_text)
    @property
    def error(self):
        error_text = self.error_text
        if not error_text: return ''
        error_lines = error_text.split('\n')
        return Html('<div class="error">%s</div>' % Html('<br>\n').join(error_lines))
    def _get_value(self):
        return (field.value for field in self.fields if not isinstance(field, Submit))
    def _set_value(self, value):
        values = list(value)
        fields = [ field for field in self.fields if not isinstance(field, Submit) ]
        if len(fields) != len(values): raise TypeError(
            'Expected sequence of %d values. Got: %d' % (len(fields), len(values)))
        for field, value in zip(fields, values): field.value = value
    value = property(_get_value, _set_value)
##  def _get_label(self, colon=True, required=False):
##      return BaseWidget._get_label(self, colon, required)
##  label = property(_get_label, BaseWidget._set_label)
    @property
    def tag(self):
        result = [ Html('\n<table><tr>') ]
        if self.show_headers:
            for i, field in enumerate(self.fields):
                if isinstance(field, Submit): label = Html('&nbsp;')
                else: label = field._get_label(colon=False)
                result.append(Html('<th>%s</th>') % label)
            result.append(Html('</tr>\n<tr>'))
        for i, field in enumerate(self.fields):
            result.append(Html('<td>%s</td>') % field.tag)
        result.append(Html('\n</tr></table>\n'))
        return htmljoin(result)
    def __unicode__(self):
        return htmljoin((self.label, self.tag, self.error))
    html = property(__unicode__)
    @property
    def hidden(self):
        return htmljoin(field.html for field in self.hidden_fields)

class Grid(BaseWidget):
    def __init__(self, label=None, columns=None, row_count=0, **attrs):
        if columns is None: raise TypeError('%s columns must be specified' % self.__class__.__name__)
        columns = list(columns)
        if 'required' in attrs: raise TypeError('%s cannot be required' % self.__class__.__name__)
        BaseWidget.__init__(self, label, None, **attrs)
        self.columns = columns
        self._rows = []
        if row_count: self.row_count = row_count
    def _init_(self, form, name, label):
        BaseWidget._init_(self, form, name, label)
        for i, row in enumerate(self._rows):
            for j, field in enumerate(row):
                if field is not None:
                    field._init_(form, '%s[%d][%d]' % (name, i, j), None)
    @property
    def col_count(self):
        return len(self.columns)
    def _get_row_count(self):
        return len(self._rows)
    __len__ = _get_row_count
    def _set_row_count(self, size):
        delta = size - len(self._rows)
        if delta < 0: self._rows[-delta:] = []
        elif delta > 0:
            for i in xrange(len(self._rows), size):
                row = tuple(Text() for column in self.columns)
                form = self.form
                if form is not None:
                    name = self.name
                    for j, field in enumerate(row):
                        field._init_(form, '%s[%d][%d]' % (name, i, j), None)
                self._rows.append(row)
    row_count = property(_get_row_count, _set_row_count)
    def __iter__(self):
        return iter(self._rows)
    def __getitem__(self, key):
        try: i, j = key
        except: return self._rows[key]
        else: return self._rows[i][j]
    def __setitem__(self, key, value):
        try: i, j = key
        except: raise TypeError('Key must be pair of integers (row_index, col_index). Got: %r' % key)
        row = list(self._rows[i])
        row[j] = value
        if value is None: pass
        elif not isinstance(value, HtmlField):
            raise TypeError('Value must be instance of HtmlField or None. Got: %r' % value)
        else: value._init_(self.form, '%s[%d][%d]' % (self.name, i, j), None)
        self._rows[i] = tuple(row)
    def _get_value(self):
        result = []
        for row in self._rows:
            values = []
            for x in row:
                if x is None: values.append(None)
                else: values.append(x.value)
            result.append(values)
        return result
    def _set_value(self, value):
        rows = list(value)
        if len(rows) != len(self._rows): raise TypeError('Incorrect row count')
        for i, row in enumerate(rows):
            if len(row) != len(self.columns):
                raise TypeError('Incorrect col count in row %d: %d' % (i, len(row)))
        for i, row, values in enumerate(izip(self._rows, rows)):
            for field, value in izip(row, values):
                if field is not None: field.value = value
    value = property(_get_value, _set_value)
    @property
    def is_valid(self):
        if not self.is_submitted or self.error_text: return False
        for row in self._rows:
            for field in row:
                if field is None: continue
                if not field.is_valid: return False
        return True
    @property
    def tag(self):
        result = [ Html('\n<table><tr>') ]
        for column in self.columns:
            result.append(Html('<th>%s</th>') % column)
        result.append(Html('</tr>\n'))
        for i, row in enumerate(self._rows):
            result.append(Html('<tr>'))
            for field in row:
                if field is None: result.append(Html('<td>&nbsp;</td>'))
                else: result.append(Html('<td>%s</td>') % field.tag)
            result.append(Html('</tr>\n'))
        result.append(Html('</table>\n'))
        return htmljoin(result)
    @property
    def hidden(self):
        return htmltag('input', name='.'+self.name, type='hidden', value='')
    