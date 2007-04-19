import re, threading

from operator import attrgetter

from pony.auth import get_ticket
from pony.templating import Html, htmljoin
from pony.web import get_request

def update_attrs(attrs1, attrs2):
    for name, value in attrs2.items():
        name = name.lower().strip('_').replace('_', '-')
        attrs1[name] = value

def add_class(attrs, cls):
    result = {}
    update_attrs(result, attrs)
    c = result.get('class')
    if c is None: result['class'] = cls
    else: result['class'] = c + ' ' + cls
    return result

def htmltag(_name_, _attrs_=None, **_attrs2_):
    attrs = {}
    if _attrs_: update_attrs(attrs, _attrs_)
    if _attrs2_: update_attrs(attrs, _attrs2_)
    attrlist = []
    make_attr = Html('%s="%s"').__mod__
    for name, value in attrs.items():
        if value is True: attrlist.append(name)
        elif value is not False and value is not None:
            attrlist.append(make_attr((name, value)))
    return Html("<%s %s>") % (_name_, Html(' ').join(attrlist))

class Form(object):
    def __init__(self, method='POST', secure=None, **attrs):
        self._request = get_request()
        self.attrs = dict((name.lower(), value)
                          for name, value in attrs.iteritems())
        self.fields = []
        self.hidden_fields = []
        self.submit_fields = []
        self._secure = False
        self._set_method(method)
        self._set_secure(secure)
        self._f = Hidden(self.attrs.get('name', ''))
    def _set_method(self, method):
        method = method.upper()
        if method not in ('GET', 'POST'): raise TypeError(
            'Invalid form method: %s (must be GET or POST)' % method)
        if method == 'GET' and self._secure:
            raise TypeError('GET form cannot be secure')
        self._method = method
        self._update_status()
    method = property(attrgetter('_method'), _set_method)
    def _set_secure(self, secure):
        if self._method == 'GET':
            if secure: raise TypeError('GET form cannot be secure')
            self._secure = False
        elif self.method == 'POST': self._secure = secure or secure is None
        else: assert False
        if self._secure: self._t = Ticket()
        elif hasattr(self, '_t'): del self._t
        self._update_status()
    secure = property(attrgetter('_secure'), _set_secure)
    def _update_status(self):
        name = self.attrs.get('name', '')
        self.is_submitted = False
        if self._request.submitted_form == name \
           and (self.method != 'POST' or self._request.method == 'POST'):
                self.is_submitted = True
    @property
    def is_valid(self):
        if not self.is_submitted: return False
        for f in self.hidden_fields:
            if not f.is_valid: return False
        for f in self.fields:
            if not f.is_valid: return False
        if self._secure and not self._request.ticket_is_valid: return None
        return True
    @property
    def error_text(self):
        for f in self.fields:
            if f.error_text: return 'Some fields below contains errors'
        if self.is_valid is None:
            return 'This form has already been submitted'
    @property
    def error(self):
        error_text = self.error_text
        if not error_text: return ''
        return Html('<div class="error">%s</div>' % error_text)
    def __setattr__(self, name, x):
        if not isinstance(x, HtmlField):
            object.__setattr__(self, name, x)
            return
        if hasattr(self, name):
            prev = getattr(self, name)
            if not isinstance(prev, HtmlField):
                raise TypeError('Invalid form field name: %s' % name)
            if   isinstance(prev, Hidden): self.hidden_fields.remove(prev)
            elif isinstance(prev, Submit): self.submit_fields.remove(prev)
            else: self.fields.remove(prev)
        if   isinstance(x, Hidden): self.hidden_fields.append(x)
        elif isinstance(x, Submit): self.submit_fields.append(x)
        else: self.fields.append(x)
        object.__setattr__(self, name, x)
        x._init_(name, self)
    def __delattr__(self, name):
        x = getattr(self, name)
        if isinstance(x, HtmlField):
            if   isinstance(x, Hidden): self.hidden_fields.remove(x)
            elif isinstance(x, Submit): self.submit_fields.remove(x)
            else: self.fields.remove(x)
        object.__delattr__(self, name)
    def header(self):
        attrs = self.attrs
        for f in self.fields:
            if isinstance(f, File):
                attrs['enctype'] = 'multipart/form-data'
                break
        result = [ htmltag('form', attrs, method=self.method,
                                          accept_charset='UTF-8') ]
        for f in self.hidden_fields: result.extend(('\n', f.html))
        result.append(self.error)
        return htmljoin(result)
    def table(self):
        result = []
        for f in self.fields:
            class_name = f.__class__.__name__.lower()
            result.extend((Html('\n<tr class="%s field">\n<th>' % class_name),
                           f.label, Html('</th>\n<td>'),
                           f.tag))
            e = f.error
            if e: result.extend((Html('&nbsp;'), e))
            result.append(Html('</td></tr>'))
        return htmljoin(result)
    def footer(self):
        result = [ f.html for f in self.submit_fields ]
        if not result: result.append(htmltag('input', type='submit'))
        result.append(Html('\n</form>'))
        return htmljoin(result)
    # def __str__(self):
    #     return unicode(self).encode('ascii', 'xmlcharrefreplace')
    def __unicode__(self):
        return Html('\n').join([ self.header(),
                                 Html('<table>'),
                                 self.table(),
                                 Html('</table>'),
                                 self.footer() ])
    html = property(__unicode__)

class HtmlField(object):
    def __init__(self, value=None):
        self.attrs = {}
        self.form = self.name = None
        self.initial_value = value
    def _init_(self, name, form):
        self.form = form
        self.name = name
    @property
    def is_submitted(self):
        return self.form._request.fields.getfirst(self.name) is not None
    @property
    def is_valid(self):
        return self.is_submitted
    def _get_value(self):
        try: return self._new_value
        except AttributeError:
            if not self.form.is_submitted: return self.initial_value
            value = self.form._request.fields.getfirst(self.name)
            if value is None: return self.initial_value
            if value is not None: value = unicode(value, 'utf8')
            return value
    def _set_value(self, value):
        self._new_value = value
    value = property(_get_value, _set_value)
    def __unicode__(self):
        value = self._get_value()
        if value is None: value = ''
        return htmltag('input', self.attrs,
                       name=self.name, value=value, type=self.HTML_TYPE)
    tag = html = property(__unicode__)
    # def __str__(self):
    #     return unicode(self).encode('ascii', 'xmlcharrefreplace')

class Hidden(HtmlField):
    HTML_TYPE = 'hidden'

class Ticket(Hidden):
    def _set_value(self, value):
        raise TypeError('Cannot set value for tickets')
    value = property(HtmlField._get_value, _set_value)
    def __unicode__(self):
        return htmltag('input', self.attrs,
                       name=self.name, value=get_ticket(), type='hidden')
    tag = html = property(__unicode__)

class Submit(HtmlField):
    HTML_TYPE = 'submit'

class Reset(HtmlField):
    HTML_TYPE = 'reset'

class BaseWidget(HtmlField):
    def __init__(self, label=None, required=False, value=None, **attrs):
        HtmlField.__init__(self, value)
        if 'id' not in attrs:
            request = get_request()
            attrs['id'] = request.id_counter.next()
        self.attrs = attrs
        self.required = required
        self._error_text = None
        self._set_label(label)
    def _init_(self, name, form):
        HtmlField._init_(self, name, form)
        if self._label == '':
            self._set_label(name.replace('_', ' ').capitalize())
    @property
    def is_valid(self):
        return self.is_submitted and not self._get_error_text()
    def _get_error_text(self):
        if self._error_text: return self._error_text
        if self.is_submitted: return self._check_error()
        return None
    def _set_error_text(self, text):
        self._error_text = text
    error_text = property(_get_error_text, _set_error_text)
    def _check_error(self):
        if self.required and not self.value: return 'This field is required'
    @property
    def error(self):
        error_text = self._get_error_text()
        if not error_text: return ''
        return Html('<span class="error">%s</span>' % (error_text))
    def _set_label(self, label):
        if label:
            if not self.required: required = ''
            else: required = Html('<sup class="required">*</sup>')
            label = (Html('<label for="%s">%s%s'
                          '<span class="colon">:</span></label>')
                     % (self.attrs['id'], label, required))
        self._label = label or ''
    label = property(attrgetter('_label'), _set_label)
    def __unicode__(self):
        return htmljoin([ self._label, self.tag, self._error ])
    html = property(__unicode__)

class File(BaseWidget):
    HTML_TYPE = 'file'
    def _get_value(self):
        if not self.form.is_submitted: return None
        fields = self.form._request.fields
        if not fields.has_key(self.name): return None
        return fields[self.name].file
    def _set_value(self, value):
        raise TypeError('This property cannot be set')
    value = property(_get_value, _set_value)
    def _get_filename(self):
        try: return self._new_value
        except AttributeError:
            if not self.form.is_submitted: return self.initial_value
            fields = self.form._request.fields
            if not fields.has_key(self.name): return None
            return fields[self.name].filename
    def _set_filename(self, filename):
        self._new_value = filename
    filename = property(_get_filename, _set_filename)
    @property
    def tag(self):
        return htmltag('input', self.attrs, name=self.name, type=self.HTML_TYPE)

class Password(BaseWidget):
    HTML_TYPE = 'password'

class Text(BaseWidget):
    HTML_TYPE = 'text'

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
        result.append(htmltag('input', name=self.name, type='hidden', value=''))
        return htmljoin(result)

class SelectWidget(BaseWidget):
    def __init__(self, label=None, required=False, value=None,
                 options=[], **attrs):
        BaseWidget.__init__(self, label, required, value, **attrs)
        self._set_options(options)
    def _set_options(self, options):
        self.keys = {}
        self.values = {}
        options = list(options)
        for i, option in enumerate(options):
            if isinstance(option, tuple):
                if len(option) == 3: key, value, description = option
                elif len(option) == 2:
                    key, description = option
                    value = unicode(key)
                else: raise TypeError('Invalid option: %r' % option)
                description = unicode(description)
            else:
                key = option
                value = description = unicode(key)
            option = key, value, description
            x = self.keys.setdefault(key, option)
            if x is not option:
                raise TypeError('Duplicate option key: %s' % key)
            x = self.values.setdefault(value, option)
            if x is not option:
                raise TypeError('Duplicate option value: %s' % value)
            options[i] = option
        self._options = tuple(options)
    options = property(attrgetter('_options'), _set_options)
    def _get_value(self): # for Select and RadioGroup
        try: return self._new_value
        except AttributeError:
            if not self.form.is_submitted: return self.initial_value
            value = self.form._request.fields.getfirst(self.name)
            if value is not None: value = unicode(value, 'utf8')
            option = self.values.get(value)
            if option is None: return None
            return option[0]
    def _set_value(self, value): # for Select and RadioGroup
        if value is not None and value not in self.keys:
            raise TypeError('Invalid widget value: %r' % value)
        self._new_value = value
    value = property(_get_value, _set_value)
    def _get_selection(self): # for Select and RadioGroup
        value = self._get_value()
        if value is None: return set()
        return set([ value ])
    def _set_selection(self, selection): # for Select and RadioGroup
        if not selection: self._new_value = None
        elif len(selection) == 1: self._set_value(iter(selection).next())
        else: raise TypeError('This type of widget '
                              'does not support multiple selection')
    selection = property(_get_selection, _set_selection)
    @property
    def tag(self): # for Select and MultiSelect
        if self.size: size = self.size
        elif not isinstance(self, MultiSelect): size = 1
        elif len(self.options) < 5: size = len(self.options)
        else: size = 5
        result = [ htmltag('select', self.attrs, name=self.name,
                           size=size, multiple=isinstance(self, MultiSelect)) ]
        selection = self._get_selection()
        for key, value, description in self.options:
            if value == description: value = None
            result.append(htmltag('option', selected=(key in selection),
                                            value=value))
            result.append(description)
            result.append(Html('</option>'))
        result.append(Html('</select>'))
        result.append(htmltag('input', name=self.name, type='hidden', value=''))
        return htmljoin(result)

class Select(SelectWidget):
    def __init__(self, label=None, required=False, value=None,
                 options=[], size=None, **attrs):
        SelectWidget.__init__(self, label, required, value, options, **attrs)
        if value is not None and value not in self.keys:
            raise TypeError('Invalid widget initial value: %r' % value)
        self.size = size

class RadioGroup(SelectWidget):
    @property
    def tag(self):
        result = [ htmltag('div', add_class(self.attrs, 'radiobuttons')) ]
        selected_key = self._get_value()
        for key, value, description in self.options:
            result.append(Html('<div class="radiobutton">'))
            result.append(htmltag('input', type='radio', name=self.name,
                                  value=value,
                                  checked=(key == selected_key)))
            result.append(Html('<span class="value">%s</span></div>')
                          % description)
        result.append(Html('</div>'))
        result.append(htmltag('input', name=self.name, type='hidden', value=''))
        return htmljoin(result)

class MultiSelect(SelectWidget):
    def __init__(self, label=None, required=False, value=None,
                 options=[] ,size=None, **attrs):
        if value is None: values = set()
        elif isinstance(value, basestring): values = set((value,))
        elif hasattr(value, '__iter__'): values = set(value)
        else: values = set((value,))
        SelectWidget.__init__(self, label, required, value, options, **attrs)
        for key in values:
            if key not in self.keys:
                raise TypeError('Invalid widget initial value: %r' % key)
        self.initial_value = values
        self.size = size
    def _get_value(self):
        raise TypeError("Use 'selection' property instead")
    def _set_value(self, value):
        raise TypeError("Use 'selection' property instead")
    value = property(_get_value, _set_value)
    def _get_selection(self):
        try: return self._new_value
        except AttributeError:
            if not self.form.is_submitted: return self.initial_value
            values = self.form._request.fields.getlist(self.name)
            if not values: return self.initial_value
            result = set()
            for value in values:
                if value is not None: value = unicode(value, 'utf8')
                option = self.values.get(value)
                if option is not None: result.add(option[0])
            return result
    def _set_selection(self, selection):
        for key in selection:
            if key not in self.keys:
                raise TypeError('Invalid widget value: %r' % key)
        self._new_value = set(selection)
    selection = property(_get_selection, _set_selection)

class CheckboxGroup(MultiSelect):
    @property
    def tag(self):
        attrs = add_class(self.attrs, 'checkboxes')
        result = [ htmltag('div', attrs) ]
        selection = self._get_selection()
        for key, value, description in self.options:
            result.append(Html('<div class="checkbox">'))
            result.append(htmltag('input', name=self.name, type='checkbox',
                                  value=value, checked=(key in selection)))
            result.append(Html('<span class="value">%s</span></div>')
                          % description)
        result.append(Html('</div>'))
        result.append(htmltag('input', name=self.name, type='hidden', value=''))
        return htmljoin(result)
    