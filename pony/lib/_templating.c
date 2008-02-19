#include "Python.h"
#include "structmember.h"

static PyTypeObject Html_Type;
static PyTypeObject StrHtml_Type;
static PyTypeObject StrHtml2_Type;
static PyTypeObject Wrapper_Type;
static PyTypeObject StrWrapper_Type;
static PyTypeObject UnicodeWrapper_Type;
static PyObject * html_make_new(PyObject *arg);
static PyObject * strhtml_make_new(PyObject *arg);
static PyObject * strwrapper_make_new(PyObject *arg);
static PyObject * wrapper_make_new(PyObject *arg);
static PyObject * unicodewrapper_make_new(PyObject *arg);
static PyObject* replace_unicode(PyObject *source);
static PyObject* replace_string(PyObject *source);

static PyObject* ex_quote(PyObject *self, PyObject *args) {
    PyObject *arg_x;

    if (!PyArg_ParseTuple(args, "O", &arg_x)) {
        return NULL;
    }
    if (PyUnicode_Check(arg_x)) {
        return replace_unicode(arg_x);
    } else {
        return replace_string(arg_x);
    }
}

static PyObject* replace_unicode(PyObject *source) {  
    Py_ssize_t i, j, size, add_size, new_size;
    Py_UNICODE *p;
    PyObject *new_string;

    assert (PyUnicode_Check(source));
    size = PyUnicode_GET_SIZE(source);
    add_size = 0;
    for (i=0; i < size; i++) {
        switch (PyUnicode_AS_UNICODE(source)[i]) {
        case '&':
        case '\'':
            add_size += 4;
            break;
        case '<':
        case '>':
            add_size += 3;
            break;
        case '"':
            add_size += 5;
            break;
        }
    }
    if (add_size == 0) {
        Py_INCREF(source);
        return (PyObject *)source;
    }
    new_size = size + add_size;
    new_string = PyUnicode_FromUnicode(NULL, new_size);
    if (new_string == NULL) return NULL;
    p = PyUnicode_AS_UNICODE(new_string);
    for (i=0, j=0; i < size; i++) {
        switch (PyUnicode_AS_UNICODE(source)[i]) {
        case '&':
            p[j++] = '&';
            p[j++] = 'a';
            p[j++] = 'm';
            p[j++] = 'p';
            p[j++] = ';';
            break;
        case '<':
            p[j++] = '&';
            p[j++] = 'l';
            p[j++] = 't';
            p[j++] = ';';
            break;
        case '>':
            p[j++] = '&';
            p[j++] = 'g';
            p[j++] = 't';
            p[j++] = ';';
            break;
        case '"':
            p[j++] = '&';
            p[j++] = 'q';
            p[j++] = 'u';
            p[j++] = 'o';
            p[j++] = 't';
            p[j++] = ';';
            break;
        case '\'':
            p[j++] = '&';
            p[j++] = '#';
            p[j++] = '3';
            p[j++] = '9';
            p[j++] = ';';
            break;
        default:
            p[j++] = PyUnicode_AS_UNICODE(source)[i];
            break;
        }
    }
    assert (j == new_size);
    return (PyObject *)new_string;
}

static PyObject* replace_string(PyObject *source) {  
    Py_ssize_t i, j, size, add_size, new_size;
    char *p;
    PyObject *new_string;

    assert (PyString_Check(source));
    size = PyString_GET_SIZE(source);
    add_size = 0;
    for (i=0; i < size; i++) {
        switch (PyString_AS_STRING(source)[i]) {
        case '&':
        case '\'':
            add_size += 4;
            break;
        case '<':
        case '>':
            add_size += 3;
            break;
        case '"':
            add_size += 5;
            break;
        }
    }
    if (add_size == 0) {
        Py_INCREF(source);
        return (PyObject *)source;
    }
    new_size = size + add_size;
    new_string = PyString_FromStringAndSize(NULL, new_size);
    if (new_string == NULL) return NULL;
    p = PyString_AS_STRING(new_string);
    for (i=0, j=0; i < size; i++) {
        switch (PyString_AS_STRING(source)[i]) {
        case '&':
            p[j++] = '&';
            p[j++] = 'a';
            p[j++] = 'm';
            p[j++] = 'p';
            p[j++] = ';';
            break;
        case '<':
            p[j++] = '&';
            p[j++] = 'l';
            p[j++] = 't';
            p[j++] = ';';
            break;
        case '>':
            p[j++] = '&';
            p[j++] = 'g';
            p[j++] = 't';
            p[j++] = ';';
            break;
        case '"':
            p[j++] = '&';
            p[j++] = 'q';
            p[j++] = 'u';
            p[j++] = 'o';
            p[j++] = 't';
            p[j++] = ';';
            break;
        case '\'':
            p[j++] = '&';
            p[j++] = '#';
            p[j++] = '3';
            p[j++] = '9';
            p[j++] = ';';
            break;
        default:
            p[j++] = PyString_AS_STRING(source)[i];
            break;
        }
    }
    assert (j == new_size);
    return (PyObject *)new_string;
}

// TODO: we do not need self here
static PyObject* html_quote(PyObject *self, PyObject *arg_x, int unicode_replace) {
    PyObject *unicode_arg, *ret;

    printf(" point A");
    if (PyObject_TypeCheck(arg_x, &PyInt_Type) ||            // if isinstance(x, (int, long, float, Html)): return x
        PyObject_TypeCheck(arg_x, &PyLong_Type) ||
        PyObject_TypeCheck(arg_x, &PyFloat_Type)||
        PyObject_TypeCheck(arg_x, &Html_Type)) {
            Py_INCREF(arg_x); 
            return arg_x;
    }
    printf("B");
    if (!PyObject_TypeCheck(arg_x, &PyBaseString_Type)) {    // if not isinstance(x, basestring):
        printf("C1");
        if (PyObject_HasAttr(arg_x, PyString_FromString("__unicode__"))) {        // if hasattr(x, '__unicode__'): x = unicode(x)
            arg_x = PyObject_Unicode(arg_x);          
            if (arg_x == NULL)
                return NULL;
        } else {
            arg_x = PyObject_Str(arg_x);                        // else: x = str(x)
        }
        if (PyObject_TypeCheck(arg_x, &Html_Type)) {
            return arg_x;
    }
    
    }
    printf("C");
    if (PyObject_TypeCheck(arg_x, &StrHtml_Type)) {
        if (unicode_replace == 0) {
            Py_INCREF(arg_x);
            return arg_x;
        }
        unicode_arg = PyUnicode_FromEncodedObject(arg_x, NULL, "replace");    //PyObject* PyUnicode_FromEncodedObject( PyObject *obj, const char *encoding, const char *errors)
        printf("C1");
        ret = html_make_new(unicode_arg);
        Py_DECREF(unicode_arg);
        printf("C2\n");
        return ret;
    }

    ///////////// repalce /////////////
    if (PyUnicode_Check(arg_x)) {
        arg_x = replace_unicode(arg_x);
    } else {
        arg_x = replace_string(arg_x);
    }
    ///////////// repalce /////////////
    printf("D");
    if (PyObject_TypeCheck(arg_x, &PyUnicode_Type)) {        // if isinstance(x, unicode): return Html(x)
        ret = html_make_new(arg_x);
        Py_DECREF(arg_x);
        return ret;
    }
    printf("E");
    if (unicode_replace == 0) {
        ret = strhtml_make_new(arg_x);
        Py_DECREF(arg_x);
        printf("E1\n");
        return ret;

    }
    unicode_arg = PyUnicode_FromEncodedObject(arg_x, NULL, "replace");    //PyObject* PyUnicode_FromEncodedObject( PyObject *obj, const char *encoding, const char *errors)
    Py_DECREF(arg_x);
    printf("F");
    ret = html_make_new(unicode_arg);
    Py_DECREF(unicode_arg);
    printf("G\n");
    return ret;
}

static PyObject* _wrap(PyObject *arg_x, int unicode_replace) {
    PyObject *unicode_arg, *ret;

    printf(" point A");
    if (PyObject_TypeCheck(arg_x, &PyInt_Type) ||            
        PyObject_TypeCheck(arg_x, &PyLong_Type) ||
        PyObject_TypeCheck(arg_x, &PyFloat_Type)) {
            Py_INCREF(arg_x); 
            return arg_x;
    }
    printf("B");
    if (!PyObject_TypeCheck(arg_x, &PyBaseString_Type)) { 
		return wrapper_make_new(arg_x, unicode_replace);
	}
	if (!PyObject_TypeCheck(arg_x, &Html_Type) &&
		!PyObject_TypeCheck(arg_x, &StrHtml_Type)) {
			if (PyUnicode_Check(arg_x)) {
				arg_x = replace_unicode(arg_x);
            } else {
                arg_x = replace_string(arg_x);
            }
		}
		if (PyObject_TypeCheck(arg_x, &PyString_Type)) {
			if (!unicode_replace) {
				ret = strwrapper_make_new(arg_x);
				Py_DECREF(arg_x);
			} else {
                unicode_arg = PyUnicode_FromEncodedObject(arg_x, NULL, "replace");   
				Py_DECREF(arg_x);
				ret = unicodewrapper_make_new(unicode_arg);
				Py_DECREF(unicode_arg);
			}
		} else {
			ret = unicodewrapper_make_new(arg_x);
			Py_DECREF(arg_x);
		}
		if (PyObject_SetAttrString(ret, "original_value", arg_x) == -1)
			return NULL;
		return ret;
}

static PyMethodDef _templating_methods[] = {
    {"quote", ex_quote, METH_VARARGS, "quote() doc string"},
    {NULL, NULL}
};


typedef struct {
    PyUnicodeObject unicode_object;
} htmlObject;

static PyObject *
html_make_new(PyObject *arg)
{
    PyObject *tup, *ret;
    if (arg == NULL)
        return NULL;
    tup = PyTuple_New(1);
    if (tup == NULL)
        return NULL;
    Py_INCREF(arg);
    PyTuple_SET_ITEM(tup, 0, arg);
    ret = PyUnicode_Type.tp_new(&Html_Type, tup, NULL);
    Py_DECREF(tup);
    return ret;
}

static PyObject *
html_add(PyObject *self, PyObject *arg)
{
    PyObject *con, *quoted, *ret;
    printf("__add__\n");
    quoted = html_quote(self, arg, 1);
    con = PyUnicode_Concat(self, quoted /*arg*/);
    Py_DECREF(quoted);
    ret = html_make_new(con);
    Py_DECREF(con);
    return ret;
}

static PyObject *
html_join(PyObject *self, PyObject *l)
{
    Py_ssize_t i;
    PyObject *quoted_list, *item, *quoted_item, *ret, *joined;

    quoted_list = PySequence_List(l);
    if (quoted_list == NULL)
        return NULL;
    for (i = 0; i < PyList_Size(l); i++) {
        item = PyList_GetItem(quoted_list, i);
        if (item == NULL) {
            Py_DECREF(quoted_list);
            return NULL;
        }
        quoted_item = html_quote(self, item, 1);
        PyList_SetItem(quoted_list, i, quoted_item);
    }
    joined = PyUnicode_Join(self, quoted_list);
    Py_DECREF(quoted_list);
    ret = html_make_new(joined);
    Py_DECREF(joined);
    return ret;
}

static PyObject *
html_repeat(PyObject *self, Py_ssize_t count)
{
    printf("__mul__\n");
    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef html_methods[] = {
    {"join", (PyCFunction)html_join, METH_O | METH_COEXIST, ""},
    {NULL, NULL}
};

static int
html_init(htmlObject *self, PyObject *args, PyObject *kwds)
{
    return 0;
}

static PyObject *
html_mod(PyObject *self, PyObject *arg_x)
{
	PyObject *x, *wrapped_item, *item, *ret, *fromatted;
	Py_ssize_t i, len;
	if (!PyObject_TypeCheck(arg_x, &PyTuple_Type)) {
		x = _wrap(arg_x, 1);
	} else {
		len = PyTuple_Size(arg_x);
		x = PyTuple_New(len);
		for (i = 0; i < len; i++) {
			item = PyTuple_GetItem(arg_x, i);
			if (item == NULL) {
				Py_DECREF(x); // TODO: DECREF for all wrapped items
				return NULL;
			}
			printf("wrappring\n");
			wrapped_item = _wrap(self, item, 1);
			printf("wrapped\n");
			PyTuple_SetItem(x, i, wrapped_item);			
		}
	}
    fromatted = PyUnicode_Format(self, x);
	if (fromatted == NULL)
		return NULL;
    Py_DECREF(x);
	ret = html_make_new(fromatted);
    Py_DECREF(x);
    return ret;
}

static PyNumberMethods html_as_number = {
    (binaryfunc)html_add,           /*nb_add*/
    0,                                /*nb_subtract*/
    0,                                /*nb_multiply*/
    0,                                /*nb_divide*/
    (binaryfunc)html_mod,            /*nb_remainder*/                // __mod__
};

static PySequenceMethods html_as_sequence = {
    0,     /* sq_length */
    0,     /* sq_concat */                // __add__
    (ssizeargfunc)html_repeat,     /* sq_repeat */      // __mul__
    0,     /* sq_item */
    0,     /* sq_slice */
    0,     /* sq_ass_item */
    0,     /* sq_ass_slice */
    0,     /* sq_contains */
};

static PyObject *
html_repr(htmlObject *self)
{
    PyObject *uc, *ret;
    uc = PyUnicode_Type.tp_repr((PyObject *)&self->unicode_object);
    ret = PyString_FromFormat("%s(%s)", ((PyObject *)self)->ob_type->tp_name, PyString_AsString(uc));
    Py_DECREF(uc);
    return ret;
}

static void
html_dealloc(PyObject *self)
{    
    PyUnicode_Type.tp_dealloc((PyObject *) self);
}

static PyTypeObject Html_Type = {
        PyObject_HEAD_INIT(NULL)
        0,                        /*ob_size*/
        "Html",                   /*tp_name*/
        sizeof(htmlObject),       /*tp_basicsize*/
        0,                        /*tp_itemsize*/
        /* methods */
        (destructor)html_dealloc, /*tp_dealloc*/  
        0,                        /*tp_print*/
        0,                        /*tp_getattr*/
        0,                        /*tp_setattr*/
        0,                        /*tp_compare*/
        (unaryfunc)html_repr,     /*tp_repr*/
        &html_as_number,          /*tp_as_number*/
        &html_as_sequence,        /*tp_as_sequence*/
        0,                        /*tp_as_mapping*/
        0,                        /*tp_hash*/
        0,                        /*tp_call*/
        0,                        /*tp_str*/
        0,                        /*tp_getattro*/
        0,                        /*tp_setattro*/
        0,                        /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT 
          | Py_TPFLAGS_CHECKTYPES,  /*tp_flags*/ 
        0,                      /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        html_methods,           /*tp_methods*/
        0,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};


typedef struct {
    PyStringObject string_object;
} strhtmlObject;

static PyObject *
strhtml_make_new(PyObject *arg)
{
    PyObject *tup, *ret;
    if (arg == NULL)
        return NULL;
    tup = PyTuple_New(1);
    if (tup == NULL)
        return NULL;
    Py_INCREF(arg);
    PyTuple_SET_ITEM(tup, 0, arg);
    ret = PyString_Type.tp_new(&StrHtml_Type, tup, NULL);
    Py_DECREF(tup);
    return ret;
}

static void
strhtml_dealloc(PyObject *self)
{    
    PyString_Type.tp_dealloc((PyObject *) self);
}

static PyObject *
strhtml_repr(strhtmlObject *self)
{
    PyObject *uc, *ret;
    uc = PyString_Type.tp_repr((PyObject *)&self->string_object);
    ret = PyString_FromFormat("%s(%s)", ((PyObject *)self)->ob_type->tp_name, PyString_AsString(uc));
    Py_DECREF(uc);
    return ret;
}

static PyObject *
strhtml_add(PyObject *self, PyObject *arg)
{
    PyObject *con, *unicode_arg, *quoted, *ret;
    printf("__add__\n");
    quoted = html_quote(self, arg, 0);
    // TODO: can html_quote raise UnicodeDecodeError?
    PyString_ConcatAndDel(&self, quoted);
    if (PyErr_Occurred()) {
        if (!PyErr_ExceptionMatches(PyExc_UnicodeDecodeError)) {
            return NULL;
        } else {
            PyErr_Clear();
            unicode_arg = PyUnicode_FromEncodedObject(self, NULL, "replace");    //PyObject* PyUnicode_FromEncodedObject( PyObject *obj, const char *encoding, const char *errors)
            // TODO: should we decrease ref to self?
            quoted = html_quote(self, arg, 1);
            con = PyUnicode_Concat(unicode_arg, quoted);
            Py_DECREF(unicode_arg);
            Py_DECREF(quoted);
            ret = html_make_new(con);
            Py_DECREF(con);
            return ret;
        }
    }
    if (PyObject_TypeCheck(self, &PyString_Type)) {
        ret = strhtml_make_new(self);
    } else {
        ret = html_make_new(self);
    }
    // TODO: is it correct?
    Py_DECREF(self);
    return ret;
}

static PyObject *
strhtml_repeat(PyObject *self, Py_ssize_t count)
{
    printf("__mul__\n");
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
strhtml_mod(PyObject *self, PyObject *arg)
{
    //if (PyObject_TypeCheck(arg_x, &PyTuple_Type)
}

static PyObject *
strhtml_join(PyObject *self, PyObject *l)
{
    Py_ssize_t i;
    PyObject *quoted_list, *item, *quoted_item, *ret, *joined;

    quoted_list = PySequence_List(l);
    if (quoted_list == NULL)
        return NULL;
    for (i = 0; i < PyList_Size(l); i++) {
        item = PyList_GetItem(quoted_list, i);
        if (item == NULL) {
            Py_DECREF(quoted_list);
            return NULL;
        }
        quoted_item = html_quote(self, item, 1);
        PyList_SetItem(quoted_list, i, quoted_item);
    }
    joined = PyUnicode_Join(self, quoted_list);
    Py_DECREF(quoted_list);
    ret = html_make_new(joined);
    Py_DECREF(joined);
    return ret;
}

static PyObject *
strhtml_str(strhtmlObject *self)
{
    PyObject *s, *ret, *tup;
    s = PyString_Type.tp_str((PyObject *)&self->string_object);
    tup = PyTuple_New(1);
    if (tup == NULL)
        return NULL;    
    PyTuple_SET_ITEM(tup, 0, s);
    ret = PyString_Type.tp_new(&StrHtml2_Type, tup, NULL);
    Py_DECREF(tup);
    return ret;
}

static PyMethodDef strhtml_methods[] = {
    {"join", (PyCFunction)html_join, METH_O | METH_COEXIST, ""},
    {NULL, NULL}
};

static PyNumberMethods strhtml_as_number = {
    (binaryfunc)strhtml_add,           /*nb_add*/
    0,                                /*nb_subtract*/
    0,                                /*nb_multiply*/
    0,                                /*nb_divide*/
    (binaryfunc)strhtml_mod,            /*nb_remainder*/                // __mod__
};

static PySequenceMethods strhtml_as_sequence = {
    0,     /* sq_length */
    0,     /* sq_concat */                // __add__
    (ssizeargfunc)strhtml_repeat,     /* sq_repeat */      // __mul__
    0,     /* sq_item */
    0,     /* sq_slice */
    0,     /* sq_ass_item */
    0,     /* sq_ass_slice */
    0,     /* sq_contains */
};

static PyTypeObject StrHtml_Type = {
        PyObject_HEAD_INIT(NULL)
        0,                        /*ob_size*/
        "StrHtml",                   /*tp_name*/
        sizeof(strhtmlObject),       /*tp_basicsize*/
        0,                        /*tp_itemsize*/
        /* methods */
        (destructor)strhtml_dealloc, /*tp_dealloc*/  
        0,                        /*tp_print*/
        0,                        /*tp_getattr*/
        0,                        /*tp_setattr*/
        0,                        /*tp_compare*/
        (unaryfunc)strhtml_repr,     /*tp_repr*/
        &strhtml_as_number,          /*tp_as_number*/
        &strhtml_as_sequence,        /*tp_as_sequence*/
        0,                        /*tp_as_mapping*/
        0,                        /*tp_hash*/
        0,                        /*tp_call*/
        (reprfunc)strhtml_str,                        /*tp_str*/
        0,                        /*tp_getattro*/
        0,                        /*tp_setattro*/
        0,                        /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT |
          Py_TPFLAGS_BASETYPE | Py_TPFLAGS_CHECKTYPES,  /*tp_flags*/ 
        0,                      /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        strhtml_methods,           /*tp_methods*/
        0,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

typedef struct {
    strhtmlObject strhtml_object;
} strhtml2Object;

static PyObject *
strhtml2_str(strhtml2Object *self)
{
    return PyString_Type.tp_str((PyObject *)&self->strhtml_object);
}


static PyTypeObject StrHtml2_Type = {
        PyObject_HEAD_INIT(NULL)
        0,                        /*ob_size*/
        "StrHtml2",                   /*tp_name*/
        sizeof(strhtml2Object),       /*tp_basicsize*/
        0,                        /*tp_itemsize*/
        /* methods */
        0,                        /*tp_dealloc*/  
        0,                        /*tp_print*/
        0,                        /*tp_getattr*/
        0,                        /*tp_setattr*/
        0,                        /*tp_compare*/
        0,                        /*tp_repr*/
        0,                        /*tp_as_number*/
        0,                        /*tp_as_sequence*/
        0,                        /*tp_as_mapping*/
        0,                        /*tp_hash*/
        0,                        /*tp_call*/
        (reprfunc)strhtml2_str,             /*tp_str*/
        0,                        /*tp_getattro*/
        0,                        /*tp_setattro*/
        0,                        /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_CHECKTYPES,  /*tp_flags*/ 
        0,                      /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        0,                      /*tp_methods*/
        0,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

typedef struct {
    PyObject_HEAD
    PyObject *value;
    PyObject *unicode_replace;
} wrapperObject;

static PyObject *
wrapper_make_new(PyTypeObject *type, PyObject *arg, int unicode_replace)
{
    wrapperObject *self;
	PyObject *value;
	self = (wrapperObject *)type->tp_alloc(type, 0);
	if (self == NULL) {
		return NULL;
	}
    self->value = value;
	self->unicode_replace = (unicode_replace == 1) ? Py_True : Py_False;
    Py_INCREF(self->value);
    Py_INCREF(self->unicode_replace);
    return (PyObject *)self;	
}

static int
wrapper_init(wrapperObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *value, *unicode_replace;
    if (!PyArg_ParseTuple(args, "OO", &value, &unicode_replace)) {
        return -1;
    }
    if (value && unicode_replace) {
        self->value = value;
        self->unicode_replace = unicode_replace;
        Py_INCREF(value);
        Py_INCREF(unicode_replace);
        return 0;
    }
    return -1;
}

static void
wrapper_dealloc(wrapperObject* self)
{
    Py_XDECREF(self->value);
    Py_XDECREF(self->unicode_replace);
    self->ob_type->tp_free((PyObject*)self);
}


static PyObject *
wrapper_unicode(wrapperObject *self, PyObject *arg)
{
    //self = (wrapperObject *)&self;
    
    return html_quote(self, self->value, (self->unicode_replace == Py_True) ? 1 : 0);
}

static PyObject *
wrapper_str(wrapperObject *self)
{
    return html_quote(self, self->value, (self->unicode_replace == Py_True) ? 1 : 0);
}

static PyObject *
wrapper_repr(wrapperObject *self)
{
    PyObject *ret, *arg;
    arg = PyObject_Repr(self->value);
    ret = html_quote(self, arg, 0);
    Py_DECREF(arg);
    return ret;
}

static PyObject *
wrapper_getitem(PyObject *self, Py_ssize_t key)
{
}

static PyMethodDef wrapper_methods[] = {
    {"__unicode__", (PyCFunction)wrapper_unicode, METH_NOARGS, ""},
    {NULL}  /* Sentinel */
};


static PyMemberDef wrapper_members[] = {
    {"value", T_OBJECT_EX, offsetof(wrapperObject, value), 0, ""},
    {"unicode_replace", T_OBJECT_EX, offsetof(wrapperObject, unicode_replace), 0, ""},
    {NULL}  /* Sentinel */
};

static PySequenceMethods wrapper_as_sequence = {
    0,     /* sq_length */
    0,     /* sq_concat */      
    0,     /* sq_repeat */      
    (ssizeargfunc)wrapper_getitem,     /* sq_item */
    0,     /* sq_slice */
    0,     /* sq_ass_item */
    0,     /* sq_ass_slice */
    0,     /* sq_contains */
};

static PyTypeObject Wrapper_Type = {
        PyObject_HEAD_INIT(NULL)
        0,                        /*ob_size*/
        "Wrapper",                /*tp_name*/
        sizeof(wrapperObject),    /*tp_basicsize*/
        0,                        /*tp_itemsize*/
        /* methods */
        (destructor)wrapper_dealloc,                        /*tp_dealloc*/  
        0,                        /*tp_print*/
        0,                        /*tp_getattr*/
        0,                        /*tp_setattr*/
        0,                        /*tp_compare*/
        (reprfunc)wrapper_repr,                        /*tp_repr*/
        0,                        /*tp_as_number*/
        &wrapper_as_sequence,                        /*tp_as_sequence*/
        0,                        /*tp_as_mapping*/
        0,                        /*tp_hash*/
        0,                        /*tp_call*/
        (reprfunc)wrapper_str,    /*tp_str*/
        0,                        /*tp_getattro*/
        0,                        /*tp_setattro*/
        0,                        /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT,  /*tp_flags*/ 
        0,                      /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        wrapper_methods,                      /*tp_methods*/
        wrapper_members,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        (initproc)wrapper_init,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0 /*wrapper_make_new*/,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

typedef struct {
    PyStringObject string_object;
} strWrapperObject;

static PyObject *
strwrapper_make_new(PyObject *arg)
{
    PyObject *tup, *ret;
    if (arg == NULL)
        return NULL;
    tup = PyTuple_New(1);
    if (tup == NULL)
        return NULL;
    Py_INCREF(arg);
    PyTuple_SET_ITEM(tup, 0, arg);
    ret = PyString_Type.tp_new(&StrWrapper_Type, tup, NULL);
    Py_DECREF(tup);
    return ret;
}

static void
strwrapper_dealloc(PyObject *self)
{    
    PyString_Type.tp_dealloc((PyObject *) self);
}

static PyObject *
strwrapper_repr(strWrapperObject *self)
{
}

static PyTypeObject StrWrapper_Type = {
        PyObject_HEAD_INIT(NULL)
        0,                        /*ob_size*/
        "StrWrapper",                   /*tp_name*/
        sizeof(strWrapperObject),       /*tp_basicsize*/
        0,                        /*tp_itemsize*/
        /* methods */
        (destructor)strwrapper_dealloc, /*tp_dealloc*/  
        0,                        /*tp_print*/
        0,                        /*tp_getattr*/
        0,                        /*tp_setattr*/
        0,                        /*tp_compare*/
        (unaryfunc)strwrapper_repr,     /*tp_repr*/
        0,          /*tp_as_number*/
        0,        /*tp_as_sequence*/
        0,                        /*tp_as_mapping*/
        0,                        /*tp_hash*/
        0,                        /*tp_call*/
        0,                        /*tp_str*/
        0,                        /*tp_getattro*/
        0,                        /*tp_setattro*/
        0,                        /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT 
          | Py_TPFLAGS_CHECKTYPES,  /*tp_flags*/ 
        0,                      /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        0,           /*tp_methods*/
        0,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

typedef struct {
    PyUnicodeObject unicode_object;
} unicodeWrapperObject;

static PyObject *
unicodewrapper_make_new(PyObject *arg)
{
    PyObject *tup, *ret;
    if (arg == NULL)
        return NULL;
    tup = PyTuple_New(1);
    if (tup == NULL)
        return NULL;
    Py_INCREF(arg);
    PyTuple_SET_ITEM(tup, 0, arg);
    ret = PyUnicode_Type.tp_new(&UnicodeWrapper_Type, tup, NULL);
    Py_DECREF(tup);
    return ret;
}

static void
unicodewrapper_dealloc(PyObject *self)
{    
    PyUnicode_Type.tp_dealloc((PyObject *) self);
}

static PyObject *
unicodewrapper_repr(unicodeWrapperObject *self)
{
}

static PyTypeObject UnicodeWrapper_Type = {
        PyObject_HEAD_INIT(NULL)
        0,                        /*ob_size*/
        "UnicodeWrapper",                   /*tp_name*/
        sizeof(unicodeWrapperObject),       /*tp_basicsize*/
        0,                        /*tp_itemsize*/
        /* methods */
        (destructor)unicodewrapper_dealloc, /*tp_dealloc*/  
        0,                        /*tp_print*/
        0,                        /*tp_getattr*/
        0,                        /*tp_setattr*/
        0,                        /*tp_compare*/
        (unaryfunc)unicodewrapper_repr,     /*tp_repr*/
        0,          /*tp_as_number*/
        0,        /*tp_as_sequence*/
        0,                        /*tp_as_mapping*/
        0,                        /*tp_hash*/
        0,                        /*tp_call*/
        0,                        /*tp_str*/
        0,                        /*tp_getattro*/
        0,                        /*tp_setattro*/
        0,                        /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT 
          | Py_TPFLAGS_CHECKTYPES,  /*tp_flags*/ 
        0,                      /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        0,           /*tp_methods*/
        0,                      /*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0,                      /*tp_alloc*/
        0,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};


PyMODINIT_FUNC
init_templating(void)
{
    PyObject* m;
    Html_Type.tp_base = &PyUnicode_Type;
    StrHtml_Type.tp_base = &PyString_Type;
    StrHtml2_Type.tp_base = &StrHtml_Type;
    Wrapper_Type.tp_new = PyType_GenericNew;
    //noddy_NoddyType.tp_new = PyType_GenericNew; // TODO: do we need that?
    if (PyType_Ready(&Html_Type) < 0)
        return;
    if (PyType_Ready(&StrHtml_Type) < 0)
        return;
    if (PyType_Ready(&StrHtml2_Type) < 0)
        return;
    if (PyType_Ready(&Wrapper_Type) < 0)
        return;
    if (PyType_Ready(&StrWrapper_Type) < 0)
        return;    
    if (PyType_Ready(&UnicodeWrapper_Type) < 0)
        return;
    m = Py_InitModule3("_templating", _templating_methods,
                       "Templating in C");

    Py_INCREF(&Html_Type);
    PyModule_AddObject(m, "Html", (PyObject *)&Html_Type);
    Py_INCREF(&StrHtml_Type);
    PyModule_AddObject(m, "StrHtml", (PyObject *)&StrHtml_Type);
    Py_INCREF(&StrHtml2_Type);
    PyModule_AddObject(m, "StrHtml2", (PyObject *)&StrHtml2_Type);
    Py_INCREF(&Wrapper_Type);
    PyModule_AddObject(m, "Wrapper", (PyObject *)&Wrapper_Type);
    Py_INCREF(&StrWrapper_Type);
    PyModule_AddObject(m, "StrWrapper", (PyObject *)&StrWrapper_Type);
    Py_INCREF(&UnicodeWrapper_Type);
    PyModule_AddObject(m, "UnicodeWrapper", (PyObject *)&UnicodeWrapper_Type);
}