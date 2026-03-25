-- ============================================================
-- PREREQUISITE: ACL grant for all REST hosts (run as SYS)
-- ============================================================
BEGIN
  --PostgreSQL via PostgREST
  DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
    host => 'postgrest-pg',
    ace  => xs$ace_type(
              privilege_list => xs$name_list('connect','resolve'),
              principal_name => 'FDBO',
              principal_type => xs_acl.ptype_db));
  --TimescaleDB via PostgREST
  DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
    host => 'postgrest-ts',
    ace  => xs$ace_type(
              privilege_list => xs$name_list('connect','resolve'),
              principal_name => 'FDBO',
              principal_type => xs_acl.ptype_db));
  --MongoDB via RESTHeart
  DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
    host => 'restheart',
    ace  => xs$ace_type(
              privilege_list => xs$name_list('connect','resolve'),
              principal_name => 'FDBO',
              principal_type => xs_acl.ptype_db));
  -- Neo4J Graph DB
  DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
    host => 'neo4j',
    ace  => xs$ace_type(
              privilege_list => xs$name_list('connect','resolve'),
              principal_name => 'FDBO',
              principal_type => xs_acl.ptype_db));
END;
/
-- ===============================================================================================================================
-- OPTIONAL: This handles basic authentification for RestHeart via HTTPURITYPE. Only checks if the database server is responsive.
-- ===============================================================================================================================
begin
  l_req  := UTL_HTTP.begin_request(pURL);
  UTL_HTTP.set_header(l_req, 'Authorization', 'Basic ' || 
    UTL_RAW.cast_to_varchar2(UTL_ENCODE.base64_encode(UTL_I18N.string_to_raw(pUserPass, 'AL32UTF8')))); 
  l_resp := UTL_HTTP.get_response(l_req);
  UTL_HTTP.READ_TEXT(l_resp, l_buffer);
  UTL_HTTP.end_response(l_resp);
  return l_buffer;
end;
/