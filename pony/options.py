# logging options:
log_to_sqlite = None
logging_level = None
logging_pony_level = None

#auth options:
auth_cookie_name = 'pony'
auth_cookie_path = '/'
auth_cookie_domain = None
auth_cookie_expires = 60*60*24*31  # one month
auth_cookie_max_age = 60*60*24*31  # one month
auth_max_ctime_diff = 60*24  # one day
auth_max_mtime_diff = 60*2  # 2 hours
auth_conversation_field_name = '_c'

# pickle options:
pickle_extension_codes_start_offset = 230
