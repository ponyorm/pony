#include "Python.h"
#include "structmember.h"

static PyTypeObject Html_Type;
static PyTypeObject StrHtml_Type;
static PyTypeObject StrHtml2_Type;
static PyTypeObject Wrapper_Type;
static PyTypeObject UnicodeWrapper_Type;
static PyObject * html_make_new(PyObject *arg);
static PyObject * strhtml_make_new(PyObject *arg);
static PyObject * wrapper_make_new(PyObject *arg);
static PyObject * unicodewrapper_make_new(PyObject *arg);
static PyObject* replace_unicode(PyObject *source);
static PyObject* replace_string(PyObject *source);
static PyObject *createStrWrapperObject(PyObject *args);

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

static PyObject* html_quote(PyObject *arg, int unicode_replace) {
    PyObject *unicode_arg, *ret;

    //printf(" point A");
    if (PyObject_TypeCheck(arg, &PyInt_Type) ||            // if isinstance(x, (int, long, float, Html)): return x
        PyObject_TypeCheck(arg, &PyLong_Type) ||
        PyObject_TypeCheck(arg, &PyFloat_Type)||
        PyObject_TypeCheck(arg, &Html_Type)) {
            Py_INCREF(arg); 
            return arg;
    }
    //printf("B");
    if (!PyObject_TypeCheck(arg, &PyBaseString_Type)) {    // if not isinstance(x, basestring):
        //printf("C1");
        if (PyObject_HasAttr(arg, PyString_FromString("__unicode__"))) {        // if hasattr(x, '__unicode__'): x = unicode(x)
            arg = PyObject_Unicode(arg);          
            if (arg == NULL)
                return NULL;
        } else {
            arg = PyObject_Str(arg);                        // else: x = str(x)
        }
        if (PyObject_TypeCheck(arg, &Html_Type)) {
            return arg;
    }
    
    }
    //printf("C");
    if (PyObject_TypeCheck(arg, &StrHtml_Type)) {
        if (unicode_replace == 0) {
            Py_INCREF(arg);
            return arg;
        }
        unicode_arg = PyUnicode_FromEncodedObject(arg, NULL, "replace");    //PyObject* PyUnicode_FromEncodedObject( PyObject *obj, const char *encoding, const char *errors)
        //printf("C1");
        ret = html_make_new(unicode_arg);
        Py_DECREF(unicode_arg);
        //printf("C2\n");
        return ret;
    }

    ///////////// repalce /////////////
    if (PyUnicode_Check(arg)) {
        arg = replace_unicode(arg);
    } else {
        arg = replace_string(arg);
    }
    ///////////// repalce /////////////
    //printf("D");
    if (PyObject_TypeCheck(arg, &PyUnicode_Type)) {        // if isinstance(x, unicode): return Html(x)
        ret = html_make_new(arg);
        Py_DECREF(arg);
        return ret;
    }
    //printf("E");
    if (unicode_replace == 0) {
        ret = strhtml_make_new(arg);
        Py_DECREF(arg);
        //printf("E1\n");
        return ret;

    }
    unicode_arg = PyUnicode_FromEncodedObject(arg, NULL, "replace");    //PyObject* PyUnicode_FromEncodedObject( PyObject *obj, const char *encoding, const char *errors)
    Py_DECREF(arg);
    //printf("F");
    ret = html_make_new(unicode_arg);
    Py_DECREF(unicode_arg);
    //printf("G\n");
    return ret;
}

static PyObject* _wrap(PyObject *arg, int unicode_replace) {
    PyObject *unicode_arg, *ret, *tmp;
    int make_dec = 0;

    printf(" point A");
    if (PyObject_TypeCheck(arg, &PyInt_Type) ||            
        PyObject_TypeCheck(arg, &PyLong_Type) ||
        PyObject_TypeCheck(arg, &PyFloat_Type)) {
            Py_INCREF(arg); 
            return arg;
    }
    printf("B");
    
    if (!PyObject_TypeCheck(arg, &PyBaseString_Type)) {
        printf("B1"); 
		return wrapper_make_new(arg, unicode_replace);
	}
	printf("C");
	if (!PyObject_TypeCheck(arg, &Html_Type) &&
		!PyObject_TypeCheck(arg, &StrHtml_Type)) {
			if (PyUnicode_Check(arg)) {
				arg = replace_unicode(arg);
            } else {
                arg = replace_string(arg);
            }
            make_dec = 1;         
		}
		
		printf("D");
		
		if (PyObject_TypeCheck(arg, &PyString_Type)) {
			if (!unicode_replace) {
			    printf("D1");
				ret = createStrWrapperObject(arg);					
			} else {
			    printf("D2");
                unicode_arg = PyUnicode_FromEncodedObject(arg, NULL, "replace"); 
                if (unicode_arg == NULL)
                    return NULL;  		
				ret = unicodewrapper_make_new(unicode_arg);
				Py_DECREF(unicode_arg);
			}
		} else {
		    printf("D3");
			ret = unicodewrapper_make_new(arg);
		}
		printf("E");
		
		if (PyObject_SetAttrString(ret, "original_value", arg) == -1) {
		    if (make_dec) 
		        Py_DECREF(arg);
			return NULL;
		}
		
		if (make_dec)
		    Py_DECREF(arg);
		
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
html_add(PyObject *arg1, PyObject *arg2)
{
    PyObject *con, *quoted1, *quoted2, *ret;
    quoted1 = html_quote(arg1, 1);
    quoted2 = html_quote(arg2, 1);
    con = PyUnicode_Concat(quoted1, quoted2);
    Py_DECREF(quoted1);
    Py_DECREF(quoted2);
    if (con == NULL) 
        return NULL;
    ret = html_make_new(con);
    Py_DECREF(con);
    return ret;
}

static PyObject *
html_join(PyObject *self, PyObject *l)
{            
    Py_ssize_t i;
    PyObject *quoted_list, *item, *quoted_item, *ret, *joined;
//printf("A");
    quoted_list = PySequence_List(l);

    if (quoted_list == NULL)
        return NULL;
    for (i = 0; i < PyList_Size(quoted_list); i++) {
        item = PyList_GetItem(quoted_list, i);
        if (item == NULL) {
            Py_DECREF(quoted_list);
            return NULL;
        }
        quoted_item = html_quote(item, 1);
        PyList_SetItem(quoted_list, i, quoted_item);
    }
    joined = PyUnicode_Join(self, quoted_list);
    Py_DECREF(quoted_list);
    ret = html_make_new(joined);
    Py_DECREF(joined);
    return ret;   
}

static PyObject *
html_repeat(htmlObject *self, Py_ssize_t count)
{
    PyObject *ret, *seq;
    seq = PyUnicode_Type.tp_as_sequence->sq_repeat(self, count);
    if (seq == NULL)
        return NULL;
    ret = html_make_new(seq);
    Py_DECREF(seq);
    return ret;
}

static PyMethodDef html_methods[] = {
    {"join", (PyCFunction)html_join, METH_O | METH_COEXIST, ""},
    {NULL, NULL}
};

static PyObject *
html_mod(PyObject *self, PyObject *arg)
{
	PyObject *x, *wrapped_item, *item, *ret, *formatted;
	Py_ssize_t i, len;
	if (!PyObject_TypeCheck(arg, &PyTuple_Type)) {
		x = _wrap(arg, 1);
	} else {
		len = PyTuple_Size(arg);
		x = PyTuple_New(len);
		for (i = 0; i < len; i++) {
			item = PyTuple_GetItem(arg, i);
			if (item == NULL) {
				Py_DECREF(x); // TODO: DECREF for all wrapped items
				return NULL;
			}
			//printf("wrappring\n");
			wrapped_item = _wrap(item, 1);
			//printf("wrapped\n");
			PyTuple_SetItem(x, i, wrapped_item);			
		}
	}
	//printf("before\n");
    formatted = PyUnicode_Format(self, x);
    //printf("after\n");
	if (formatted == NULL)
		return NULL;
    Py_DECREF(x);
    //printf("before return\n");
	ret = html_make_new(formatted);
    Py_DECREF(formatted);
    //printf("return\n");
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
    uc = PyUnicode_Type.tp_repr(self);
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
strhtml_str(strhtmlObject *self)
{
    PyObject *s, *ret, *tup;
    printf("str\n");
    s = PyString_Type.tp_str(self);
    tup = PyTuple_New(1);
    if (tup == NULL)
        return NULL;    
    PyTuple_SET_ITEM(tup, 0, s);
    ret = PyString_Type.tp_new(&StrHtml2_Type, tup, NULL);
    Py_DECREF(tup);
    return ret;
}

static PyObject *
strhtml_repr(strhtmlObject *self)
{
    PyObject *uc, *ret;
    printf("repr\n");
    uc = PyString_Type.tp_repr(self);
    ret = PyString_FromFormat("%s(%s)", ((PyObject *)self)->ob_type->tp_name, PyString_AsString(uc));
    Py_DECREF(uc);
    return ret;
}

static PyObject *
strhtml_add(PyObject *arg1, PyObject *arg2)
{
    PyObject *con, *unicode_arg, *quoted, *ret, *arg1copy;    
    quoted = html_quote(arg2, 0);
    // TODO: can html_quote raise UnicodeDecodeError?
    arg1copy = PyString_FromString(PyString_AsString(arg1));
    PyString_ConcatAndDel(&arg1copy, quoted);
    if (PyErr_Occurred()) {
        if (!PyErr_ExceptionMatches(PyExc_UnicodeDecodeError)) {
            return NULL;
        } else {
            PyErr_Clear();
            unicode_arg = PyUnicode_FromEncodedObject(arg1, NULL, "replace");    //PyObject* PyUnicode_FromEncodedObject( PyObject *obj, const char *encoding, const char *errors)
            // TODO: should we decrease ref to self?
            quoted = html_quote(arg2, 1);
            con = PyUnicode_Concat(unicode_arg, quoted);
            Py_DECREF(unicode_arg);
            Py_DECREF(quoted);
            ret = html_make_new(con);
            Py_DECREF(con);
            return ret;
        }
    }
    if (PyObject_TypeCheck(arg1copy, &PyString_Type)) {
        ret = strhtml_make_new(arg1);
    } else {
        ret = html_make_new(arg1);
    }
    // TODO: is it correct?
    Py_DECREF(arg1copy);
    return ret;
}

static PyObject *
strhtml_repeat(PyObject *self, Py_ssize_t count)
{
    PyObject *ret, *seq;
    seq = PyString_Type.tp_as_sequence->sq_repeat(self, count);
    if (seq == NULL)
        return NULL;
    if (PyObject_TypeCheck(seq, &PyString_Type)) {
        ret = strhtml_make_new(seq);
    } else {
        ret = html_make_new(seq);
    }    
    Py_DECREF(seq);
    return ret;
}

static PyObject *
strhtml_mod(PyObject *self, PyObject *arg)
{
	PyObject *x, *wrapped_item, *item, *ret, *formatted, *unicode_arg, *unicode_self;
	Py_ssize_t i, len;
	printf("A");
	if (!PyObject_TypeCheck(arg, &PyTuple_Type)) {
		x = _wrap(arg, 0);
		printf("B");
	} else {
		len = PyTuple_Size(arg);
		x = PyTuple_New(len);
		for (i = 0; i < len; i++) {
			item = PyTuple_GetItem(arg, i);
			if (item == NULL) {
				Py_DECREF(x); // TODO: DECREF for all wrapped items
				return NULL;
			}
			wrapped_item = _wrap(item, 0);
			printf("wrapped\n");			
			PyTuple_SetItem(x, i, wrapped_item);			
		}
	}
    formatted = PyString_Format(self, x);
    printf("C");
    Py_DECREF(x);
    if (PyErr_Occurred()) {
        printf("ERR");
        if (!PyErr_ExceptionMatches(PyExc_UnicodeDecodeError)) {
            return NULL;
        } else {
            PyErr_Clear();
            Py_DECREF(x);
	        if (!PyObject_TypeCheck(arg, &PyTuple_Type)) {
		        x = _wrap(arg, 1);
		        printf("B");
	        } else {
		        len = PyTuple_Size(arg);
		        x = PyTuple_New(len);
		        for (i = 0; i < len; i++) {
			        item = PyTuple_GetItem(arg, i);
			        if (item == NULL) {
				        Py_DECREF(x); // TODO: DECREF for all wrapped items
				        return NULL;
			        }
			        wrapped_item = _wrap(item, 1);
			        printf("wrapped\n");			
			        PyTuple_SetItem(x, i, wrapped_item);			
		        }
	        }            
            // -- Html(unicode.__mod__(unicode(self, errors='replace'), y))
            unicode_self = PyUnicode_FromEncodedObject(self, NULL, "replace");
            formatted = PyUnicode_Format(unicode_self, x);
            Py_DECREF(unicode_self);
            if (formatted == NULL) // TODO: goto error and clean everything
	            return NULL;
            ret = html_make_new(formatted);
            Py_DECREF(formatted);
            return ret;
        }
    }
	if (PyObject_TypeCheck(formatted, &PyString_Type)) {
	    printf("D");
        ret = strhtml_make_new(formatted);
	    Py_DECREF(formatted);
        return ret;
	} else {
	    printf("E");
        ret = html_make_new(formatted);
	    Py_DECREF(formatted);
        return ret;
	}
}

static PyObject *
strhtml_join(PyObject *self, PyObject *l)
{
    Py_ssize_t i;
    PyObject *quoted_list, *item, *quoted_item, *ret, *joined, *unicode_self;

    printf("A");
    quoted_list = PySequence_List(l);
    if (quoted_list == NULL)
        return NULL;
    for (i = 0; i < PyList_Size(quoted_list); i++) {
        item = PyList_GetItem(quoted_list, i);
        if (item == NULL) {
            Py_DECREF(quoted_list);
            return NULL;
        }
        quoted_item = html_quote(item, 0);
        PyList_SetItem(quoted_list, i, quoted_item);
    }
    printf("C");
    joined = _PyString_Join(self, quoted_list);
    printf("D");
    if (PyErr_Occurred()) {
        printf("ERR");
        if (!PyErr_ExceptionMatches(PyExc_UnicodeDecodeError)) {
            Py_DECREF(quoted_list);
            return NULL;
        } else {
            PyErr_Clear();            
            // return Html(unicode(self, errors='replace')).join(items)
            unicode_self = PyUnicode_FromEncodedObject(self, NULL, "replace");
            joined = PyUnicode_Join(unicode_self, quoted_list);
            Py_DECREF(quoted_list);
            Py_DECREF(unicode_self);
            ret = html_make_new(joined);
            Py_DECREF(joined);
            return ret;
        }
    }
    if (PyObject_TypeCheck(joined, &PyString_Type)) {
        ret = strhtml_make_new(joined);
    } else {
        ret = html_make_new(joined);
    } 
    Py_DECREF(quoted_list);
    Py_DECREF(joined);
    return ret;
}

static PyMethodDef strhtml_methods[] = {
    {"join", (PyCFunction)strhtml_join, METH_O | METH_COEXIST, ""},
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
    return PyString_Type.tp_str(self);
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
wrapper_make_new(PyTypeObject *type, PyObject *value, int unicode_replace)
{
    wrapperObject *self;
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
    return html_quote(self->value, (self->unicode_replace == Py_True) ? 1 : 0);
}

static PyObject *
wrapper_str(wrapperObject *self)
{
    return html_quote(self->value, (self->unicode_replace == Py_True) ? 1 : 0);
}

static PyObject *
wrapper_repr(wrapperObject *self)
{
    PyObject *ret, *arg;
    arg = PyObject_Repr(self->value);
    ret = html_quote(arg, 0);
    Py_DECREF(arg);
    return ret;
}

static PyObject *
wrapper_getitem(PyObject *self, Py_ssize_t key)
{
    PyObject *item, *ret;
    item = PyObject_GetItem(((wrapperObject *)self)->value, Py_BuildValue("i",key));
    if (item == NULL)
        return NULL;
    ret = _wrap(item, ((wrapperObject *)self)->unicode_replace ? Py_True : Py_False);
    Py_DECREF(item);
    return ret;
}

static PyMethodDef wrapper_methods[] = {
    {"__unicode__", (PyCFunction)wrapper_unicode, METH_NOARGS, ""},
    {NULL} 
};


static PyMemberDef wrapper_members[] = {
    {"value", T_OBJECT_EX, offsetof(wrapperObject, value), 0, ""},
    {"unicode_replace", T_OBJECT_EX, offsetof(wrapperObject, unicode_replace), 0, ""},
    {NULL} 
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

static PyObject *
createStrWrapperObject(PyObject *arg)
{    
    PyObject *pmod, *pinst, *pclass, *tup;
    // TODO: optimize with static field
    pmod   = PyImport_ImportModule("pony.templating");
    if (pmod == NULL) {
        return NULL;        
    }
    pclass = PyObject_GetAttrString(pmod, "StrWrapper");   
    if (pclass == NULL) {
        return NULL;
    }
    tup = PyTuple_New(1);
    if (tup == NULL) {
        return NULL;
    }
    Py_INCREF(arg);
    PyTuple_SET_ITEM(tup, 0, arg);

    pinst  = PyEval_CallObject(pclass, tup);      
    Py_DECREF(tup);
    if (pinst == NULL) {
        return NULL;        
    }
    Py_DECREF(pmod);
    Py_DECREF(pclass);
    return pinst;
}

typedef struct {
    PyUnicodeObject unicode_object;
    PyObject *original_value;
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
unicodewrapper_dealloc(unicodeWrapperObject *self)
{
    Py_XDECREF(self->original_value);    
    PyUnicode_Type.tp_dealloc((PyObject *) self);
}

static PyObject *
unicodewrapper_repr(unicodeWrapperObject *self)
{
    PyObject *ret, *arg;
    arg = PyObject_Repr(self->original_value);
    //printf("REPR=%s\n", PyString_AsString(arg));
    ret = html_quote(arg, 0);
    Py_DECREF(arg);
    return ret;
}

static PyMemberDef unicodewrapper_members[] = {
    {"original_value", T_OBJECT_EX, offsetof(unicodeWrapperObject, original_value), 0, ""},
    {NULL}  
};

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
        unicodewrapper_members,                      /*tp_members*/
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
    UnicodeWrapper_Type.tp_base = &PyUnicode_Type;    
        
    if (PyType_Ready(&Html_Type) < 0)
        return;
    if (PyType_Ready(&StrHtml_Type) < 0)
        return;
    if (PyType_Ready(&StrHtml2_Type) < 0)
        return;
    if (PyType_Ready(&Wrapper_Type) < 0)
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
    Py_INCREF(&UnicodeWrapper_Type);
    PyModule_AddObject(m, "UnicodeWrapper", (PyObject *)&UnicodeWrapper_Type);
}