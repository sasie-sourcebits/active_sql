/*   
         Sasie 
         sasieindrajit@gmail.com    
*/

#define IfNILorSTRING(obj)	(NIL_P(obj)? NULL: StringValuePtr(obj))
#define IfNILorINT(obj)		(NIL_P(obj)? 0: NUM2INT(obj))

struct config {
  char *ser, *user, *pass, *db, *sock;
  unsigned int pp, flg;
};

struct tmpCon {
  VALUE host, user, passwd, db, port, sock, flag;
};

struct mysql {
  MYSQL *conn;
  unsigned int reconnect;
  unsigned int adds;
};

struct config myConfig;
struct mysql mysObj;
struct tmpCon tCon;


static VALUE activeSQL;
static VALUE rb_cBase;
static VALUE rb_eSqlError;
static VALUE rb_cCols;

void Init_activesql();

// Definition section.
static void init_active_sqls();
static VALUE activeSQL_load_config(int argc, VALUE *argv, VALUE self);
static VALUE activeSQL_version(VALUE self);
static VALUE activeSQL_connect(VALUE self);
static VALUE activeSQL_reconnect(VALUE self);
static VALUE activeSQL_disconnect(VALUE self);
static VALUE activeSQL_connected(VALUE self);
static VALUE activeSQL_exec(VALUE self, VALUE arg, VALUE ret_vl);
static VALUE activeSQL_set_chars_set(int argc, VALUE *argv, VALUE self);
static VALUE activeSQL_exec_sql(VALUE self, VALUE arg);
static int castPreWrk(enum enum_field_types typ);
static VALUE castFrFetch(unsigned int typ, VALUE row, unsigned int itr);
static VALUE activeSQL_dyamic_meth(VALUE self);
static VALUE activeSQL_add_methods(int argc, VALUE *argv, VALUE self);
static VALUE activeSQL_added_methods(VALUE self);
static VALUE activeSQL_my_database(int argc, VALUE *argv, VALUE self);
static VALUE activeSQL_my_select(VALUE self);
static VALUE activeSQL_auto_commit(VALUE self, VALUE arg);

// Def.
static VALUE activeSQL_auto_commit(VALUE self, VALUE arg) {
 //if (mysql_autocommit(mysObj.conn, 1) == 0)
 //    return Qtrue; Do it from query.
}

static VALUE activeSQL_my_select(VALUE self) {
  return rb_reg_new("\\A\\s*(SELECT|SHOW|DESCRIBE|EXPLAIN|CHECK TABLE|DESC|CALL)", 57, 1);
}

static VALUE activeSQL_my_database(int argc, VALUE *argv, VALUE self) {
  VALUE cdb;
  if (!mysObj.reconnect) {
    rb_scan_args(argc, argv, "01", &cdb);
    if (TYPE(cdb) == T_STRING)
      if (mysql_select_db(mysObj.conn, RSTRING_PTR(cdb)) == 0)
        return Qtrue;
      else
        return Qfalse;
    else
      return Qfalse;
  } 
  else
    rb_raise(rb_eSqlError, "Connection not established.");
}

static VALUE activeSQL_added_methods(VALUE self) {
  if (mysObj.adds == 0)
    return Qtrue;
  else
    return Qfalse;
}

static VALUE activeSQL_add_methods(int argc, VALUE *argv, VALUE self) {
  VALUE needed;
  rb_scan_args(argc, argv, "01", &needed);
  if (TYPE(needed) == T_TRUE) {
    mysObj.adds = 0;
    return Qtrue;
  } 
  else if (TYPE(needed) == T_FALSE) {
    mysObj.adds = 1;
    return Qtrue;
  }
  else
    return Qfalse;
}

static VALUE activeSQL_dyamic_meth(VALUE self) {
  //unsigned int ind = findIndex();
  //return RARRAY(self)->ptr[ind];
}

static VALUE activeSQL_exec_sql(VALUE self, VALUE arg) {
  VALUE f;
  if (mysObj.reconnect == 1)
    f = rb_funcall(rb_cBase, rb_intern("connect!"), 0);
  else
    f = Qtrue;
  if (TYPE(f) == T_TRUE) {
    if (TYPE(arg) != T_STRING)
      rb_raise(rb_eTypeError, "invalid type for input");
    if (mysql_real_query(mysObj.conn, RSTRING_PTR(arg), (unsigned int) strlen(RSTRING_PTR(arg))))
      rb_raise(rb_eSqlError, mysql_error(mysObj.conn));
    else
      return Qtrue;
  }
  else
    rb_raise(rb_eSqlError, "Connection not established.");
}

static VALUE activeSQL_set_chars_set(int argc, VALUE *argv, VALUE self) {
  VALUE cset;
  if (!mysObj.reconnect) {
    rb_scan_args(argc, argv, "01", &cset);
    if (TYPE(cset) == T_STRING)
      if (!mysql_set_character_set(mysObj.conn, RSTRING_PTR(cset)))
        return rb_str_new2(mysql_character_set_name(mysObj.conn));
      else
        return Qfalse;
      else
        return rb_str_new2(mysql_character_set_name(mysObj.conn));
  } 
  else
    rb_raise(rb_eSqlError, "Connection not established.");
}

static VALUE activeSQL_exec(VALUE self, VALUE arg, VALUE ret_vl) {
  unsigned int * castAs;
  unsigned int i, n, fld_cnt, j;
  unsigned long * lengths;
  char **getters;
  char *tbl_name;
  char *dyn_method;
  MYSQL_RES *res;
  MYSQL_ROW row;
  MYSQL_FIELD *fields;
  VALUE ary, f, cmd;
  VALUE res_set; //Colllect result to send it back.
  if (mysObj.reconnect == 1)
    f = rb_funcall(rb_cBase, rb_intern("connect!"), 0);
  else
    f = Qtrue;
  if (TYPE(f) == T_TRUE) {
    if (TYPE(arg) != T_STRING)
      rb_raise(rb_eTypeError, "invalid type for input");
    if (mysql_real_query(mysObj.conn, RSTRING_PTR(arg), (unsigned int) strlen(RSTRING_PTR(arg)))) {
      rb_raise(rb_eSqlError, mysql_error(mysObj.conn));
    }
    if (TYPE(ret_vl) == T_TRUE) {
      res = mysql_use_result(mysObj.conn);
      res_set = rb_ary_new();
      fld_cnt = mysql_num_fields(res);
      fields = mysql_fetch_fields(res);
      ary = rb_ary_new2(fld_cnt);

      /* ALLOCATE ROOM : DYN METHODS FOR GETTERS */
      getters = (char **) malloc(fld_cnt * sizeof (char *));
      castAs = (unsigned int*) malloc(fld_cnt * sizeof (int));

      for (i = 0; i < fld_cnt; i++) {
        getters[i] = fields[i].name;
        tbl_name = fields[i].org_name ? fields[i].org_name : fields[i].name;
        strcat(tbl_name, "@");
        strcat(tbl_name, fields[i].org_table ? fields[i].org_table : fields[i].table);
        rb_ary_store(ary, i, rb_tainted_str_new2(tbl_name));
        tbl_name = "";
        castAs[i] = castPreWrk(fields[i].type);
      }
      rb_ary_push(res_set, ary);
      while ((row = mysql_fetch_row(res)) != NULL) {
        n = mysql_num_fields(res);
        lengths = mysql_fetch_lengths(res);
        ary = rb_ary_new2(n);
        for (i = 0; i < n; i++) {
          rb_ary_store(ary, i, row[i] ? castFrFetch(castAs[i], rb_tainted_str_new(row[i], lengths[i]), i) : Qnil);
          //rb_define_singleton_method(ary, "{dynamic getters}", activeSQL_dyamic_meth, 0); WE CN ADD IF HV NEW WAY. 
          if (mysObj.adds == 0)
            if (getters[i] != "") {
              dyn_method = (char *) malloc(((unsigned int) strlen(getters[i]) + 21)); // **
              sprintf(dyn_method, "def %s; self[%u]; end", getters[i], i);
              cmd = rb_str_new2(dyn_method);
              rb_obj_instance_eval(1, &cmd, ary);
              free(dyn_method);
            }
          }
        rb_ary_push(res_set, ary);
      }

      // FREE all.
      free(getters);
      free(castAs);
      mysql_free_result(res);
      return res_set;
      }
      else {
        res = mysql_use_result(mysObj.conn); // To avoid out of sync error from api.
        mysql_free_result(res);
        return Qtrue; /* Add affacted rows instead of status if needed */
     }
  }
  else
    rb_raise(rb_eSqlError, "Connection not established.");
}

static VALUE activeSQL_connected(VALUE self) {
  if (mysObj.reconnect == 0)
    return Qtrue;
  else
    return Qfalse;
}

static VALUE activeSQL_disconnect(VALUE self) {
  mysObj.reconnect = 1;
  mysql_close(mysObj.conn);
  return Qtrue;
}

static VALUE activeSQL_reconnect(VALUE self) {
  //TODO: disconect should happen once.
  rb_funcall(rb_cBase, rb_intern("disconnect!"), 0);
  return rb_funcall(rb_cBase, rb_intern("connect!"), 0);
}

static VALUE activeSQL_connect(VALUE self) {
  if (mysObj.reconnect == 1) {
  mysObj.conn = mysql_init(NULL);
  if (!mysql_real_connect(mysObj.conn, myConfig.ser, myConfig.user, myConfig.pass, myConfig.db, myConfig.pp, myConfig.sock, myConfig.flg)) {
    fprintf(stderr, "%s\n", mysql_error(mysObj.conn));
    return Qfalse;
  }
  else {
    mysObj.reconnect = 0;
    return Qtrue;
  }
  }
  else
    return Qfalse;
}

static VALUE activeSQL_version(VALUE self) {
  return rb_str_new("1.0.1", 5);
}

static VALUE activeSQL_load_config(int argc, VALUE *argv, VALUE self) {
  rb_scan_args(argc, argv, "07", &tCon.host, &tCon.user, &tCon.passwd, &tCon.db, &tCon.port, &tCon.sock, &tCon.flag);
  myConfig.db = IfNILorSTRING(tCon.db);
  myConfig.flg = IfNILorINT(tCon.flag);
  myConfig.ser = IfNILorSTRING(tCon.host);
  myConfig.user = IfNILorSTRING(tCon.user);
  myConfig.pass = IfNILorSTRING(tCon.passwd);
  myConfig.pp = IfNILorINT(tCon.port);
  myConfig.sock = IfNILorSTRING(tCon.sock);
  mysObj.reconnect = 1;
  return Qnil;
}


// activeSQL 
void Init_activesql() {
  activeSQL = rb_define_module("ActiveSQL");
  rb_cBase = rb_define_class_under(activeSQL, "Base", rb_cObject);
  rb_eSqlError = rb_define_class("ActiveSQLError", rb_eStandardError);
  rb_cCols = rb_define_class_under(activeSQL, "Cols", rb_cArray);
  // methods visible.
  rb_define_singleton_method(rb_cBase, "load_config", activeSQL_load_config, -1);
  rb_define_singleton_method(rb_cBase, "version", activeSQL_version, 0);
  rb_define_singleton_method(rb_cBase, "connect!", activeSQL_connect, 0);
  rb_define_singleton_method(rb_cBase, "reconnect!", activeSQL_reconnect, 0);
  rb_define_singleton_method(rb_cBase, "disconnect!", activeSQL_disconnect, 0);
  rb_define_singleton_method(rb_cBase, "connected?", activeSQL_connected, 0);
  rb_define_singleton_method(rb_cBase, "exec", activeSQL_exec, 2);
  rb_define_singleton_method(rb_cBase, "my_char_set", activeSQL_set_chars_set, -1);
  rb_define_singleton_method(rb_cBase, "exec_sql", activeSQL_exec_sql, 1);
  rb_define_singleton_method(rb_cBase, "add_methods", activeSQL_add_methods, -1);
  rb_define_singleton_method(rb_cBase, "add_methods?", activeSQL_added_methods, 0);
  rb_define_singleton_method(rb_cBase, "my_database", activeSQL_my_database, -1);
  rb_define_singleton_method(rb_cBase, "my_select?", activeSQL_my_select, 0);
  //rb_define_singleton_method(rb_cBase, "auto_commit", activeSQL_auto_commit, 1);
  init_active_sqls();
}

// Will work behind.
// static VALUE castFrFetch(unsigned int typ, char ** row, unsigned long len, unsigned int itr){

static VALUE castFrFetch(unsigned int typ, VALUE row, unsigned int itr) {
  VALUE cst;
  switch (typ){
    case 1:
      cst = rb_funcall2(row, rb_intern("to_i"), 0, NULL);
      break;
    case 2:
      cst = rb_funcall2(row, rb_intern("to_f"), 0, NULL);
      break;
    case 3:
      default:
      return row;
  }
  return cst;
}

static int castPreWrk(enum enum_field_types typ) {
  unsigned int ret;
  switch (typ) {
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_LONG:
      ret = 1;
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      ret = 2;
      break;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_NULL:
      ret = 3;
      break;
    default:
      ret = 3;
  }
  return ret;
}

static void init_active_sqls() {
  mysObj.reconnect = 1;
  // Append methods as getters.
  mysObj.adds = 0;
}

