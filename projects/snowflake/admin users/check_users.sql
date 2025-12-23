-- Define type user between 'PERSON' or 'SERVICE', conform documentaion snowflake.
ALTER USER VITOROLIVEIRA SET TYPE = 'PERSON';


-- Check Type User in Snowflake and verify type e if have MFA active.
SELECT
    NAME,
    LOGIN_NAME,
    TYPE,
    HAS_MFA,
    EMAIL,
    DEFAULT_WAREHOUSE,
    DEFAULT_NAMESPACE,
    DEFAULT_ROLE,
    LAST_SUCCESS_LOGIN
FROM snowflake.account_usage.users
WHERE 
  TYPE = 'PERSON' 
  AND NAME ='NAME_USERS'
  AND DISABLED = FALSE
  AND DELETED_ON IS NULL
  AND HAS_MFA = FALSE
  AND TYPE IS NULL
  AND TYPE = 'PERSON'
  AND TYPE = 'SERVICE'
ORDER BY NAME;
