-- STEP 01: If user contein MFA atived, first remove polivy mfa.

-- Remove the policy of authenticarion of user
ALTER USER IF EXISTS 'USER_NAME' UNSET AUTHENTICATION POLICY;

-- STEP 02: Use these two codes to disable user's MFA.
ALTER USER 'USER_NAME' UNSET DISABLE_MFA;
ALTER USER 'USER_NAME' SET DISABLE_MFA = TRUE;
