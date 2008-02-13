#include "Python.h"

static PyTypeObject Html_Type;
static PyObject * html_make_new(PyObject *arg);
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

static PyObject* html_quote(PyObject *self, PyObject *arg_x) {
    PyObject *unicode_arg, *ret;
    char unicode_replace = 0;

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
    }
	if (PyObject_TypeCheck(arg_x, &Html_Type)) {
		return arg_x;
	}
    printf("C");
	/*
    if (PyObject_IsInstance(arg_x, strhtmlclass)) {            // if isinstance(x, StrHtml):
        printf(" point E\n");
	}
	*/

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
	/*
    if (!unicode_replace) { // if not unicode_replace: return StrHtml(x)
		//
    }
	*/
    unicode_arg = PyUnicode_FromEncodedObject(arg_x, NULL, "replace");    //PyObject* PyUnicode_FromEncodedObject( PyObject *obj, const char *encoding, const char *errors)
	printf("F");
	ret = html_make_new(unicode_arg);
	printf("G\n");
	Py_DECREF(arg_x);
	Py_DECREF(unicode_arg);
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
	quoted = html_quote(self, arg);
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
		quoted_item = html_quote(self, item);
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
html_mod(PyObject *self, PyObject *arg)
{
    //if (PyObject_TypeCheck(arg_x, &PyTuple_Type)
}

static PyNumberMethods html_as_number = {
    (binaryfunc)html_add,           /*nb_add*/
    0,								/*nb_subtract*/
    0,								/*nb_multiply*/
    0,								/*nb_divide*/
    (binaryfunc)html_mod,			/*nb_remainder*/                // __mod__
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
		0,						/*ob_size*/
		"Html",					/*tp_name*/
		sizeof(htmlObject),		/*tp_basicsize*/
		0,						/*tp_itemsize*/
		/* methods */
		(destructor)html_dealloc,/*tp_dealloc*/  
		0,						/*tp_print*/
		0,                      /*tp_getattr*/
		0,						/*tp_setattr*/
		0,						/*tp_compare*/
		(unaryfunc)html_repr,   /*tp_repr*/
		&html_as_number,        /*tp_as_number*/
		&html_as_sequence,      /*tp_as_sequence*/
		0,						/*tp_as_mapping*/
		0,						/*tp_hash*/
		0,                      /*tp_call*/
        0,                      /*tp_str*/
        0,						/*tp_getattro*/
        0,                      /*tp_setattro*/
        0,                      /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT |
          Py_TPFLAGS_BASETYPE | Py_TPFLAGS_CHECKTYPES,     /*tp_flags*/ 
        0,                      /*tp_doc*/
        0,                      /*tp_traverse*/
        0,                      /*tp_clear*/
        0,                      /*tp_richcompare*/
        0,                      /*tp_weaklistoffset*/
        0,                      /*tp_iter*/
        0,                      /*tp_iternext*/
        html_methods,           /*tp_methods*/
        0,						/*tp_members*/
        0,                      /*tp_getset*/
        0,                      /*tp_base*/
        0,                      /*tp_dict*/
        0,                      /*tp_descr_get*/
        0,                      /*tp_descr_set*/
        0,                      /*tp_dictoffset*/
        0,                      /*tp_init*/
        0, //PyType_GenericAlloc,                      /*tp_alloc*/
        0, //PyType_GenericNew,                      /*tp_new*/
        0,                      /*tp_free*/
        0,                      /*tp_is_gc*/
};

PyMODINIT_FUNC
init_templating(void)
{
    PyObject* m;
    Html_Type.tp_base = &PyUnicode_Type;
    //noddy_NoddyType.tp_new = PyType_GenericNew; // TODO: do we need that?
    if (PyType_Ready(&Html_Type) < 0)
        return;

    m = Py_InitModule3("_templating", _templating_methods,
                       "Templating in C");

    Py_INCREF(&Html_Type);
    PyModule_AddObject(m, "Html", (PyObject *)&Html_Type);
}