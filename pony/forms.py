# -*- encoding: cp1251 -*-

from __future__ import absolute_import, print_function
from pony.py23compat import izip, iteritems

import re, threading, os.path, copy, cPickle

from operator import attrgetter
from itertools import count, cycle
import datetime

from pony import auth
from pony.utils import decorator
from pony.converting import str2py, ValidationError, converters, str2date
from pony.templating import Html, StrHtml, htmljoin, htmltag, html
from pony.web import http
from pony.webutils import component

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
def _form_init_decorator(__init__, form, *args, **kwargs):
    try: init_counter = form._init_counter
    except AttributeError:
        if form.__class__ is not Form: Form.__init__.original_func(form)
        object.__setattr__(form, '_init_args', cPickle.dumps((args, kwargs), 2))
        object.__setattr__(form, '_init_counter', 1)
    else: object.__setattr__(form, '_init_counter', init_counter+1)
    try: __init__(form, *args, **kwargs)
    finally:
        init_counter = form._init_counter
        object.__setattr__(form, '_init_counter', init_counter-1)

http_303_incompatible_browsers = []

DEFAULT = object()

class Form(object):
    __metaclass__ = FormMeta
    def __setattr__(form, name, x):
        prev = getattr(form, name, None)
        if not isinstance(x, HtmlField):
            if isinstance(prev, HtmlField): form.__delattr__(name)
            object.__setattr__(form, name, x)
            return
        if name == 'submit': raise TypeError('Invalid form field name: submit')
        if hasattr(form, name):
            if not isinstance(prev, HtmlField):
                raise TypeError('Invalid form field name: %s' % name)
            try:
                # try..except is necessary because __init__ can be called twice
                if   isinstance(prev, Hidden): form.hidden_fields.remove(prev)
                elif isinstance(prev, Submit): form.submit_fields.remove(prev)
                else: form.fields.remove(prev)
            except ValueError: pass
        if   isinstance(x, Hidden): form.hidden_fields.append(x)
        elif isinstance(x, Submit): form.submit_fields.append(x)
        else: form.fields.append(x)
        object.__setattr__(form, name, x)
        x._init_(form, name, name.replace('_', ' ').capitalize())
    def __delattr__(form, name):
        x = getattr(form, name)
        if isinstance(x, HtmlField):
            try:
                # try..except is necessary because __init__ can be called twice
                if   isinstance(x, Hidden): form.hidden_fields.remove(x)
                elif isinstance(x, Submit): form.submit_fields.remove(x)
                else: form.fields.remove(x)
            except ValueError: pass
        object.__delattr__(form, name)
    def __init__(form, method='GET', secure=DEFAULT,
                 prevent_resubmit=True, buttons_align=None, **attrs):
        # Note for subclassers: __init__ can be called twice!
        object.__setattr__(form, '_pickle_entire_form', False)
        object.__setattr__(form, '_cleared', False)
        object.__setattr__(form, '_validated', False)
        object.__setattr__(form, '_error_text', None)
        object.__setattr__(form, '_request', http.request)
        object.__setattr__(form, 'attrs', dict((name.lower(), str(value))
                                               for name, value in iteritems(attrs)))
        object.__setattr__(form, 'fields', [])
        object.__setattr__(form, 'hidden_fields', [])
        object.__setattr__(form, 'submit_fields', [])
        object.__setattr__(form, '_secure', False)
        form._set_method(method)
        if secure is DEFAULT: secure = (method=='POST')
        form._set_secure(secure)
        object.__setattr__(form, 'prevent_resubmit', prevent_resubmit)
        object.__setattr__(form, 'buttons_align', buttons_align)
        if form.__class__ is not Form and 'name' not in attrs: form.attrs['name'] = form.__class__.__name__
        name = form.attrs.get('name')
        if name: form._f = Hidden(name)
    def __getstate__(form):
        state = form._init_args
        if form._pickle_entire_form or state is None:
            state = form.__dict__.copy()
            for attr in ('_pickle_entire_form', '_init_args', '_init_counter',
                         '_cleared', '_validated', '_error_text', '_request', 'is_submitted'):
                state.pop(attr, None)
        return state
    def __setstate__(form, state):
        if isinstance(state, str):
            args, kwargs = cPickle.loads(state)
            form.__init__(*args, **kwargs)
        elif isinstance(state, dict):
            state['_pickle_entire_form'] = True
            state['_init_args'] = None
            state['_init_counter'] = 0
            state['_cleared'] = state['_validated'] = False
            state['_error_text'] = None
            state['_request'] = http.request
            form.__dict__.update(state)
            form._update_status()
        else: assert False  # pragma: no cover
    def _handle_request_(form):
        request = http.request
        if not form.is_valid:
            request.form_processed = False
            return
        try: without_redirect = form.on_submit()
        except FormNotProcessed: request.form_processed = False
        else:
            if request.form_processed is None: request.form_processed = True
            if without_redirect: return
            user_agent = request.environ.get('HTTP_USER_AGENT', '')
            for browser in http_303_incompatible_browsers:
                if browser in user_agent: raise http.Redirect('.', status='302 Found')
            raise http.Redirect(request.full_url, status='303 See Other')
    def _get_data(form):
        result = {}
        for f in form.hidden_fields:
            if f.name not in ('_f', '_t'): result[f.name] = f.value
        for f in form.fields: result[f.name] = f.value
        return result
    def _set_data(form, d):
        fields = []
        for name, value in d.items():
            f = getattr(form, name, None)
            if f: fields.append(f)
            else: raise ValueError("There is no field named '%s' in the form" % name)
        for f in fields:
            f.value = d[f.name]
    data = property(_get_data, _set_data)
    def clear(form):
        object.__setattr__(form, '_cleared', True)
        object.__setattr__(form, 'is_submitted', False)
    def _set_method(form, method):
        method = method.upper()
        if method == 'GET': form.secure = False
        elif method != 'POST': raise TypeError('Invalid form method: %s (must be GET or POST)' % method)
        object.__setattr__(form, '_method', method)
        form._update_status()
    method = property(attrgetter('_method'), _set_method)
    def _set_secure(form, secure):
        if secure == form._secure: return
        if secure and form._method == 'GET': raise TypeError('GET form cannot be secure')
        object.__setattr__(form, '_secure', secure)
        if form._secure: form._t = Ticket()
        elif hasattr(form, '_t'): del form._t
        form._update_status()
    secure = property(attrgetter('_secure'), _set_secure)
    def _update_status(form):
        object.__setattr__(form, 'is_submitted', False)
        request = form._request
        if form._cleared or request.form_processed: return
        if request.form_processed: return
        if request.submitted_form != form.attrs.get('name'): return
        object.__setattr__(form, 'is_submitted', True)
    @property
    def is_valid(form):
        if not form.is_submitted: return False
        if form.method == 'POST' and http.request.method != 'POST': return False
        form._validate()
        if form._error_text: return False
        for f in form.hidden_fields:
            if not f.is_valid: return False
        for f in form.fields:
            if not f.is_valid: return False
        if form._secure and not auth.local.ticket:
            return auth.local.ticket  # may be False or None
        return True
    def _validate(form):
        if form._validated: return
        object.__setattr__(form, '_validated', True)
        form.validate()
    def validate(form):
        pass
    def _get_error_text(form):
        if not form.is_submitted: return None
        if form._cleared or form._request.form_processed: return None
        if form._error_text is not None: return form._error_text
        form._validate()
        for f in form.fields:
            if f.error_text: return html('@{Some fields below contain errors}')
        if form.is_valid is None: return html('@{The form has already been submitted}')
    def _set_error_text(form, text):
        object.__setattr__(form, '_error_text', text)
    error_text = property(_get_error_text, _set_error_text)
    @property
    def error(form):
        error_text = form.error_text
        if not error_text: return ''
        return Html('\n<div class="error">%s</div>' % error_text)
    @property
    def tag(form):
        attrs = form.attrs
        for f in form.fields:
            if isinstance(f, File):
                attrs['enctype'] = 'multipart/form-data'
                break
        error_class = 'has-error' if form.error_text else ''
        return htmltag('form', attrs, method=form.method, accept_charset='UTF-8',
                       _class=('pony ' + error_class).strip())
    @property
    def header(form):
        result = [ form.tag ]
        for f in form.hidden_fields: result.append(f.html)
        for f in form.fields:
            hidden = f.hidden
            if hidden: result.append(hidden)
        return Html('\n') + Html('\n').join(result)
    @property
    def footer(form):
        return Html('</form>')
    @property
    def table(form):
        result = []
        for f in form.fields:
            classes = f.__class__.__name__.lower() + '-field-row'
            if f.error_text: classes += ' has-error'
            result.extend((Html('\n<tr class="%s">\n<th>' % classes),
                           f.label, Html('</th>\n<td>'), f.tag))
            error = f.error
            if error: result.append(error)
            result.append(Html('</td></tr>'))
        return htmljoin(result)
    @property
    def buttons(form):
        if not form.submit_fields: return ''
        result = [ htmltag('div', _class='buttons', align=form.buttons_align) ]
        buttons = [ f.html for f in form.submit_fields ]
        result.extend(buttons)
        result.append(Html('\n</div>'))
        return htmljoin(result)
    def __str__(form):
        return StrHtml(unicode(form).encode('ascii', 'xmlcharrefreplace'))
    def __unicode__(form):
        if form.buttons_align is None:
              buttons = Html('\n<tr><td>&nbsp;</td><td>%s</td></tr>') % form.buttons
        else: buttons = Html('\n<tr><td colspan="2">%s</td></tr>') % form.buttons
        return htmljoin([ form.header, form.error,
                          Html('\n<table>'),
                          form.table,
                          buttons,
                          Html('\n</table>\n'),
                          form.footer,
                          Html('\n')])
    html = property(__unicode__)
Form.NotProcessed = FormNotProcessed
Form.DoNotDoRedirect = DoNotDoRedirect = True

class HtmlField(object):
    def __init__(field, value=None, **attrs):
        if 'type' in attrs: raise TypeError('You can set type only for Text fields')
        if 'regex' in attrs: raise TypeError('You can set regex only for Text fields')
        field.attrs = attrs
        field.form = field.name = None
        field.initial_value = value
        field._label = None
    def _init_(field, form, name, label):
        object.__setattr__(form, '_validated', False)
        field.form = form
        field.name = name
        if field._label is None: field._label = label
    def __getstate__(field):
        state = field.__dict__.copy()
        # state.pop('_initial_value', None)
        state.pop('_new_value', None)
        return state
    def __setstate__(field, state):
        field.__dict__.update(state)
        # field._initial_value = None
    @property
    def is_submitted(field):
        form = field.form
        if form is None or not form.is_submitted: return False
        fields = form._request.fields
        if fields.getfirst(field.name) is not None: return True
        return fields.getfirst('.' + field.name) is not None
    @property
    def is_valid(field):
        return field.is_submitted
    def _get_value(field):
        try: return field._new_value
        except AttributeError:
            if not field.is_submitted: return field.initial_value
            value = field.form._request.fields.getfirst(field.name)
            if value is None: return None
            try: return unicode(value, 'utf8')
            except UnicodeDecodeError: raise http.BadRequest
    def _set_value(field, value):
        form = field.form
        if form is None or form._init_counter: field.initial_value = value
        else:
            field._new_value = value
            object.__setattr__(form, '_validated', False)
    value = property(_get_value, _set_value)
    html_value = property(_get_value)
    def __unicode__(field):
        value = field.html_value
        if value is None: value = ''
        return htmltag('input', field.attrs, name=field.name, value=value, type=field.HTML_TYPE,
                       _class_='%s-field' % field.__class__.__name__.lower())
    tag = html = property(__unicode__)
    def __str__(field):
        return StrHtml(unicode(field).encode('ascii', 'xmlcharrefreplace'))
    def __repr__(field):
        return '<%s: %s>' % (field.name or '?', field.__class__.__name__)

class Hidden(HtmlField):
    HTML_TYPE = 'hidden'

class Ticket(Hidden):
    def _get_value(field):
        form = field.form
        if form is not None and hasattr(form, 'on_submit'): payload = cPickle.dumps(form, 2)
        else: payload = None
        return auth.get_ticket(payload, form.prevent_resubmit)
    def _set_value(field, value):
        raise TypeError('Cannot set value for tickets')
    value = property(_get_value, _set_value)
    html_value = property(_get_value)

class Submit(HtmlField):
    HTML_TYPE = 'submit'
    def _init_(field, form, name, label, **attrs):
        HtmlField._init_(field, form, name, label, **attrs)
        if field.initial_value is None: field.initial_value = label

class Reset(Submit):
    HTML_TYPE = 'reset'

class BaseWidget(HtmlField):
    def __init__(field, label=None, required=None, value=None, **attrs):
        if 'id' not in attrs: attrs['id'] = next(http.response.id_counter)
        HtmlField.__init__(field, value, **attrs)
        field.required = required
        field._error_text = None
        field._auto_error_text = None
        field._set_label(label)
    def __getstate__(field):
        dict = HtmlField.__getstate__(field)
        # dict.pop('_label', None)
        dict.pop('_error_text', None)
        dict.pop('_auto_error_text', None)
        return dict
    def __setstate__(field, state):
        HtmlField.__setstate__(field, state)
        # field.label = None
        field._error_text = None
        field._auto_error_text = None
    @property
    def is_valid(field):
        return field.is_submitted and not field.error_text
    def _get_error_text(field):
        form = field.form
        if form is None or form._cleared or form._request.form_processed: return None
        if field._error_text: return field._error_text
        if field.is_submitted: return field._check_error()
        return None
    def _set_error_text(field, text):
        field._error_text = text
    error_text = property(_get_error_text, _set_error_text)
    def _check_error(field):
        value = field.value
        if field._auto_error_text: return field._auto_error_text
        if field.required and not value: return html('@{This field is required}')
    @property
    def error(field):
        error_text = field.error_text
        if not error_text: return ''
        return Html('<div class="error">%s</div>') % error_text
    def _get_label(field, colon=True, required=True):
        if not field._label: return ''
        if not (required and field.required): required_html = ''
        else: required_html = Html('<sup class="required">*</sup>')
        colon_html = Html('<span class="colon">:</span>') if colon else ''
        return Html('<label for="%s">%s%s%s</label>') % (
            field.attrs['id'], field._label, required_html, colon_html)
    def _set_label(field, label):
        field._label = label
    label = property(_get_label, _set_label)
    def __unicode__(field):
        return htmljoin((field.label, field.tag, field.error))
    html = property(__unicode__)
    @property
    def hidden(field):
        if not field.attrs.get('disabled'): return ''
        value = field.html_value
        if value is None: value = ''
        return htmltag('input', type='hidden', name=field.name, value=value)

class File(BaseWidget):
    HTML_TYPE = 'file'
    def __init__(field, label=None, required=None, **attrs):
        if 'value' in attrs: raise TypeError('Cannot set value of File field')
        BaseWidget.__init__(field, label, required, **attrs)
    def _init_(field, form, name, label):
        if form.method != 'POST': raise TypeError('Only form with method="POST" can contain File fields')
        BaseWidget._init_(field, form, name, label)
    def _get_value(field):
        if not field.is_submitted: return None
        fields = field.form._request.fields
        try: filename = fields[field.name].filename
        except: return None
        if not filename: return None
        return fields[field.name].file
    def _set_value(field, value):
        raise TypeError('This property cannot be set')
    value = property(_get_value, _set_value)
    @property
    def filename(field):
        if not field.is_submitted: return None
        fields = field.form._request.fields
        try: filename = fields[field.name].filename
        except: return None
        if not filename: return None
        return os.path.basename(filename)
    @property
    def tag(field):
        return htmltag('input', field.attrs, name=field.name, type=field.HTML_TYPE)

class Password(BaseWidget):
    HTML_TYPE = 'password'

class Text(BaseWidget):
    HTML_TYPE = 'text'
    def __init__(field, label=None, required=None, value=None, type=None, regex=None, **attrs):
        BaseWidget.__init__(field, label, required, value, **attrs)
        if isinstance(type, basestring) and type not in converters:
            raise TypeError('Unknown field type value: %r' % type)
        elif isinstance(type, tuple):
            if len(type) == 2: type += (None,)
            elif len(type) != 3:
                raise TypeError('Type tuple length must be 2 or 3. Got: %d' % len(type))
        field.type = type
        if isinstance(regex, basestring): regex = re.compile(regex, re.UNICODE)
        field.regex = regex
    def _get_value(field):
        value = BaseWidget._get_value(field)
        if value is None: return None
        if field.regex is not None:
            match = field.regex.match(value)
            if match is None:
                field._auto_error_text = html('@{Invalid data}')
                return None
        try: return str2py(value, field.type)
        except ValidationError as e:
            err_msg = e.args[0]
            translated_msg = html('@{%s}' % err_msg)  # possible template injection?
            field._auto_error_text = translated_msg
            return None
    value = property(_get_value, BaseWidget._set_value)
    @property
    def html_value(field):
        value = BaseWidget._get_value(field)
        type = field.type
        if value is None or type is None or isinstance(value, unicode): return value
        if isinstance(type, tuple): str2py, py2str, err_msg = type
        else: str2py, py2str, err_msg = converters.get(type, (field.type, unicode, None))
        return py2str(value)

class DatePicker(Text):
    def __init__(field, label=None, required=None, value=None, **attrs):
        if 'type' in attrs: raise TypeError("You can not set 'type' attribute for DatePicker")
        if 'regex' in attrs: raise TypeError("You can not set 'regex' attribute for DatePicker")
        Text.__init__(field, label, required, value, **attrs)

    @property
    @component(css=[ ('/pony/static/jquery/ui.datepicker.css', 'print, projection, screen'),
                     ('/pony/static/jquery/ui.datepicker-ie.css', 'projection, screen', 'if lte IE 7') ],
                 js=[ '/pony/static/jquery/jquery.js',
                      '/pony/static/jquery/ui.core.js',
                      '/pony/static/jquery/ui.datepicker.js',
                      '/pony/static/js/datepicker.js' ])
    def tag(field):
        return Text.tag.fget(field)
    def _get_value(field):
        value = BaseWidget._get_value(field)
        if not value: return None
        try: return str2date(value)
        except: field._auto_error_text = html('@{Incorrect date}')
        return None
    value = property(_get_value, Text._set_value)
    @property
    def html_value(field):
        value = Text._get_value(field)
        if isinstance(value, datetime.date): return value.strftime('%m/%d/%Y')
        if value is None: return value
        return unicode(value)

class StaticText(BaseWidget):
    def __init__(field, value, **attrs):
        if 'label' in attrs: raise TypeError("You can not set 'label' attribute for StaticText")
        if 'required' in attrs: raise TypeError("You can not set 'required' attribute for StaticText")
        BaseWidget.__init__(field, None, None, value, **attrs)
    def __unicode__(field):
        return Html('<strong>%s</strong>') % field.value
    html = tag = property(__unicode__)
    @property
    def is_valid(field):
        return not field.error_text

class TextArea(BaseWidget):
    @property
    def tag(field):
        result = [ htmltag('textarea', field.attrs, name=field.name) ]
        if field.value is not None: result.append(field.value)
        result.append(Html('</textarea>'))
        return htmljoin(result)

class Checkbox(BaseWidget):
    HTML_TYPE = 'checkbox'
    def _get_value(field):
        return bool(BaseWidget._get_value(field))
    def _set_value(field, value):
        BaseWidget._set_value(field, bool(value))
    value = property(_get_value, _set_value)
    @property
    def tag(field):
        result = []
        result.append(htmltag('input', field.attrs, name=field.name,
                              value='yes', checked=bool(field.value),
                              type = field.HTML_TYPE))
        return htmljoin(result)
    @property
    def hidden(field):
        return htmltag('input', name='.'+field.name, type='hidden', value='')

class Select(BaseWidget):
    def __init__(field, label=None, required=False, value=None, options=[], **attrs):
        BaseWidget.__init__(field, label, required, **attrs)
        field._set_options(options)
        field.value = value
        size = attrs.get('size')
        if size is not None: pass
        elif not isinstance(field, MultiSelect): field.attrs['size'] = 1
        else: field.attrs['size'] = min(len(field.options), 5)
    def _set_options(field, options):
        field.keys = {}
        field.values = {}
        options = list(options)
        for i, option in enumerate(options):
            if isinstance(option, tuple):
                if len(option) == 3:
                    value, description, key = option
                    key = unicode(key)
                elif len(option) == 2:
                    value, description = option
                    key = unicode(value)
                else: raise TypeError('Invalid option: %r' % option)
                description = unicode(description)
            else:
                value = option
                key = description = unicode(value)
            option = value, description, key
            x = field.keys.setdefault(key, option)
            if x is not option: raise TypeError('Duplicate option key: %s' % key)
            x = field.values.setdefault(value, option)
            if x is not option: raise TypeError('Duplicate option value: %s' % value)
            options[i] = option
        field._options = tuple(options)
        form = field.form
        if form is not None: object.__setattr__(form, '_validated', False)
    options = property(attrgetter('_options'), _set_options)
    def _get_value(field): # for Select and RadioGroup
        try: return field._new_value
        except AttributeError:
            if not field.is_submitted: return field.initial_value
            key = field.form._request.fields.getfirst(field.name)
            if key is None: return None
            try: key = unicode(key, 'utf8')
            except UnicodeDecodeError: raise http.BadRequest
            option = field.keys.get(key)
            if option is None: return None
            return option[0]
    def _set_value(field, value): # for Select and RadioGroup
        if value is not None and value not in field.values:
            raise TypeError('Invalid widget value: %r' % value)
        form = field.form
        if form is None or form._init_counter: field.initial_value = value
        else:
            field._new_value = value
            object.__setattr__(form, '_validated', False)
    value = property(_get_value, _set_value)
    @property
    def tag(field): # for Select and MultiSelect
        result = [ htmltag('select', field.attrs, name=field.name, multiple=isinstance(field, MultiSelect)) ]
        value = field.value
        if isinstance(field, MultiSelect): selection = value
        elif value is None: selection = set()
        else: selection = set((value,))
        for value, description, key in field.options:
            if key == description: key = None
            result.append(htmltag('option', selected=(value in selection), value=key))
            result.append(description)
            result.append(Html('</option>'))
        result.append(Html('</select>'))
        return htmljoin(result)
    @property
    def hidden(field):
        if field.__class__ == Select and str(field.attrs.get('size', '')) == '1': return ''
        return htmltag('input', name='.'+field.name, type='hidden', value='')

class AutoSelect(Select):
    def __init__(field, label=None, required=False, value=None, options=[], **attrs):
        Select.__init__(field, label, required, value, options, onchange='this.form.submit()', **attrs)
    @property
    def tag(field):
        return Select.tag.fget(field) + Html('\n<noscript>\n'
                                            '<input type="submit" value="apply">\n'
                                            '</noscript>\n')

class RadioGroup(Select):
    @property
    def tag(field):
        result = [ htmltag('div', field.attrs, _class='radiobuttons') ]
        selected = field.value
        for value, description, key in field.options:
            result.append(Html('<div class="radiobutton">'))
            result.append(htmltag('input', type='radio', name=field.name,
                                  value=key, checked=(value==selected)))
            result.append(Html('<span class="value">%s</span></div>') % description)
        result.append(Html('</div>'))
        result.append(htmltag('input', name='.'+field.name, type='hidden', value=''))
        return htmljoin(result)

class MultiSelect(Select):
    def _get_value(field):
        try: return field._new_value
        except AttributeError:
            if not field.is_submitted: return field.initial_value.copy()
            keys = field.form._request.fields.getlist(field.name)
            result = set()
            for key in keys:
                try: key = unicode(key, 'utf8')
                except UnicodeDecodeError: raise http.BadRequest
                option = field.keys.get(key)
                if option is not None: result.add(option[0])
            return result
    def _set_value(field, value):
        if value is None: values = set()
        elif isinstance(value, basestring): values = set((value,))
        elif hasattr(value, '__iter__'): values = set(value)
        else: values = set((value,))
        for value in values:
            if value not in field.values: raise TypeError('Invalid widget value: %r' % value)
        form = field.form
        if form is None or form._init_counter: field.initial_value = values
        else:
            field._new_value = values
            object.__setattr__(form, '_validated', False)
    value = property(_get_value, _set_value)

class CheckboxGroup(MultiSelect):
    @property
    def tag(field):
        result = [ htmltag('div', field.attrs, _class='checkboxes') ]
        selection = field.value
        for value, description, key in field.options:
            result.append(Html('<div class="checkboxgroup-item">'))
            result.append(htmltag('input', name=field.name, type='checkbox',
                                  value=value, checked=(value in selection)))
            result.append(Html('<span class="value">%s</span></div>') % description)
        result.append(Html('</div>'))
        result.append(htmltag('input', name='.'+field.name, type='hidden', value=''))
        return htmljoin(result)

class Composite(BaseWidget):
    def __init__(composite, label=None, required=None, show_headers=True, **attrs):
        BaseWidget.__init__(composite, label, required, **attrs)
        composite.show_headers = show_headers
        composite.hidden_fields = []
        composite.fields = []
    def __setattr__(composite, name, x):
        prev = getattr(composite, name, None)
        if not isinstance(x, HtmlField):
            if isinstance(prev, HtmlField): composite.__delattr__(name)
            object.__setattr__(composite, name, x)
            return
        if composite.form is None: raise TypeError('You must first assign the Composite object to the form')
        if hasattr(composite, name):
            if not isinstance(prev, HtmlField): raise TypeError('Invalid composite item name: %s' % name)
            elif isinstance(prev, Hidden): composite.hidden_fields.remove(prev)
            else: composite.fields.remove(prev)
        if composite.required is not None and x.required is None: x.required = composite.required
        if isinstance(x, Hidden): composite.hidden_fields.append(x)
        else: composite.fields.append(x)
        object.__setattr__(composite, name, x)
        field_name = '%s.%s' % (composite.name, name)
        field_label = name.replace('_', ' ').capitalize()
        x._init_(composite.form, field_name, field_label)
    def __delattr__(composite, name):
        x = getattr(composite, name)
        if isinstance(x, Hidden): composite.hidden_fields.remove(x)
        elif isinstance(x, HtmlField): composite.fields.remove(x)
        object.__delattr__(composite, name)
    @property
    def is_submitted(composite):
        form = composite.form
        if form is None or not form.is_submitted: return False
        for field in composite.fields:
            if field.is_submitted: return True
        return False
    def _get_error_text(composite):
        form = composite.form
        if form is None or not form.is_submitted: return None
        if form._cleared or form._request.form_processed: return None
        if composite._error_text: return composite._error_text
        result = []
        for field in composite.fields:
            if isinstance(field, Submit): continue
            error_text = field.error_text
            if not error_text: continue
            result.append('%s: %s' % (field._label, error_text))
        result = '\n'.join(result)
        if result.isspace(): return None
        return result
    error_text = property(_get_error_text, BaseWidget._set_error_text)
    @property
    def error(composite):
        error_text = composite.error_text
        if not error_text: return ''
        error_lines = error_text.split('\n')
        return Html('<div class="error">%s</div>' % Html('<br>\n').join(error_lines))
    def _get_value(composite):
        return (field.value for field in composite.fields if not isinstance(field, Submit))
    def _set_value(composite, value):
        values = list(value)
        fields = [ field for field in composite.fields if not isinstance(field, Submit) ]
        if len(fields) != len(values): raise TypeError(
            'Expected sequence of %d values. Got: %d' % (len(fields), len(values)))
        for field, value in izip(fields, values): field.value = value
    value = property(_get_value, _set_value)
##  def _get_label(composite, colon=True, required=False):
##      return BaseWidget._get_label(composite, colon, required)
##  label = property(_get_label, BaseWidget._set_label)
    @property
    def tag(composite):
        result = [ Html('\n<table><tr>') ]
        if composite.show_headers:
            for i, field in enumerate(composite.fields):
                if isinstance(field, Submit): label = Html('&nbsp;')
                else: label = field._get_label(colon=False)
                result.append(Html('<th>%s</th>') % label)
            result.append(Html('</tr>\n<tr>'))
        for i, field in enumerate(composite.fields):
            result.append(Html('<td>%s</td>') % field.tag)
        result.append(Html('\n</tr></table>\n'))
        return htmljoin(result)
    def __unicode__(composite):
        return htmljoin((composite.label, composite.tag, composite.error))
    html = property(__unicode__)
    @property
    def hidden(composite):
        return htmljoin(field.html for field in composite.hidden_fields)

class Grid(BaseWidget):
    def __init__(grid, label=None, columns=None, row_count=0, **attrs):
        if columns is None: raise TypeError('%s columns must be specified' % grid.__class__.__name__)
        columns = list(columns)
        if 'required' in attrs: raise TypeError('%s cannot be required' % grid.__class__.__name__)
        BaseWidget.__init__(grid, label, None, **attrs)
        grid.columns = columns
        grid._rows = []
        if row_count: grid.row_count = row_count
    def _init_(grid, form, name, label):
        BaseWidget._init_(grid, form, name, label)
        for i, row in enumerate(grid._rows):
            for j, field in enumerate(row):
                if field is not None:
                    field._init_(form, '%s[%d][%d]' % (name, i, j), None)
    @property
    def col_count(grid):
        return len(grid.columns)
    def _get_row_count(grid):
        return len(grid._rows)
    __len__ = _get_row_count
    def _set_row_count(grid, size):
        delta = size - len(grid._rows)
        if delta < 0: grid._rows[delta:] = []
        elif delta > 0:
            for i in xrange(len(grid._rows), size):
                row = tuple(Text() for column in grid.columns)
                form = grid.form
                if form is not None:
                    name = grid.name
                    for j, field in enumerate(row):
                        field._init_(form, '%s[%d][%d]' % (name, i, j), None)
                grid._rows.append(row)
    row_count = property(_get_row_count, _set_row_count)
    def __iter__(grid):
        return iter(grid._rows)
    def __getitem__(grid, key):
        try: i, j = key
        except: return grid._rows[key]
        else: return grid._rows[i][j]
    def __setitem__(grid, key, value):
        try: i, j = key
        except: raise TypeError('Key must be pair of integers (row_index, col_index). Got: %r' % key)
        row = list(grid._rows[i])
        row[j] = value
        if value is None: pass
        elif not isinstance(value, HtmlField):
            raise TypeError('Value must be instance of HtmlField or None. Got: %r' % value)
        else: value._init_(grid.form, '%s[%d][%d]' % (grid.name, i, j), None)
        grid._rows[i] = tuple(row)
    def _get_value(grid):
        result = []
        for row in grid._rows:
            values = []
            for x in row:
                if x is None: values.append(None)
                else: values.append(x.value)
            result.append(values)
        return result
    def _set_value(grid, value):
        rows = list(value)
        if len(rows) != len(grid._rows): raise TypeError('Incorrect row count')
        for i, row in enumerate(rows):
            if len(row) != len(grid.columns):
                raise TypeError('Incorrect col count in row %d: %d' % (i, len(row)))
        for i, row, values in enumerate(izip(grid._rows, rows)):
            for field, value in izip(row, values):
                if field is not None: field.value = value
    value = property(_get_value, _set_value)
    @property
    def is_valid(grid):
        if not grid.is_submitted or grid.error_text: return False
        for row in grid._rows:
            for field in row:
                if field is None: continue
                if not field.is_valid: return False
        return True
    @property
    def tag(grid):
        result = [ Html('\n<table><tr>') ]
        for column in grid.columns:
            result.append(Html('<th>%s</th>') % column)
        result.append(Html('</tr>\n'))
        for row, row_class in izip(grid._rows, cycle(('odd', 'even'))):
            result.append(Html('<tr class="%s">') % row_class)
            for field in row:
                if field is None: result.append(Html('<td>&nbsp;</td>'))
                else: result.append(Html('<td>%s</td>') % field.tag)
            result.append(Html('</tr>\n'))
        result.append(Html('</table>\n'))
        return htmljoin(result)
    @property
    def hidden(grid):
        result = [ htmltag('input', name='.'+grid.name, type='hidden', value='') ]
        for row in grid._rows:
            for field in row:
                hidden = getattr(field, 'hidden', None)
                if hidden: result.append(hidden)
        return Html('\n').join(result)
