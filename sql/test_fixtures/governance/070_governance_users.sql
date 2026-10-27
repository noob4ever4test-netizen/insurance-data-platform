CREATE USER IF NOT EXISTS azki_governance_test_admin IDENTIFIED WITH no_password DEFAULT ROLE role_admin;
CREATE USER IF NOT EXISTS azki_governance_test_engineer IDENTIFIED WITH no_password DEFAULT ROLE role_engineer;
CREATE USER IF NOT EXISTS azki_governance_test_analyst IDENTIFIED WITH no_password DEFAULT ROLE role_analyst;
CREATE USER IF NOT EXISTS azki_governance_test_partner_1 IDENTIFIED WITH no_password DEFAULT ROLE role_partner_1;
CREATE USER IF NOT EXISTS azki_governance_test_partner_2 IDENTIFIED WITH no_password DEFAULT ROLE role_partner_2;
ALTER QUOTA azki_governance_analyst_quota TO azki_governance_test_analyst;
