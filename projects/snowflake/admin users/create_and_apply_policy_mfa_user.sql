-- Crate policy MFA to user, use MFA in DUO APP conform documentaion of snowflake recomend
CREATE OR REPLACE AUTHENTICATION POLICY MFA_FORTALECENDO_AUTENTICACAO_USUARIO
    AUTHENTICATION_METHODS = ('SAML', 'PASSWORD')
    MFA_AUTHENTICATION_METHODS = ('SAML', 'PASSWORD') -- enforce Snowflake MFA for native passwords only
    MFA_ENROLLMENT = 'REQUIRED'
    COMMENT = 'Política de autenticação usando senha e MFA-DUO';

-- Check Policy Create
DESC AUTHENTICATION POLICY MFA_FORTALECENDO_AUTENTICACAO_USUARIO;

-- Aplly Policy in user, it will be activated on the next login.
ALTER USER 'USER_NAME' SET AUTHENTICATION POLICY MFA_FORTALECENDO_AUTENTICACAO_USUARIO;

-- Check Apply Police aplicated
SHOW AUTHENTICATION POLICIES ON USER 'USER_NAME';
