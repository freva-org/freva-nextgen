#!/usr/bin/env bash
set -euo pipefail

KEYCLOAK_HOST=${KEYCLOAK_HOST:-localhost:8080}
ADMIN_USER=${KEYCLOAK_ADMIN:-keycloak}
ADMIN_PASSWORD=${KEYCLOAK_ADMIN_PASSWORD:-secret}
REALM=freva
NEW_USER=${USER:-$(whoami)}
NEW_PASSWORD=secret

echo "Waiting for Keycloak to become available ..."
until curl -s -o /dev/null -w "%{http_code}" "http://$KEYCLOAK_HOST/realms/freva/.well-known/openid-configuration" | grep -q "^200$"; do
    sleep 2
done

echo "Keycloak is up. Getting admin token..."

ADMIN_TOKEN=$(curl -s \
  -d "username=$ADMIN_USER" \
  -d "password=$ADMIN_PASSWORD" \
  -d 'grant_type=password' \
  -d 'client_id=admin-cli' \
  "http://$KEYCLOAK_HOST/realms/master/protocol/openid-connect/token" |
  jq -r .access_token)

if [ -z "$ADMIN_TOKEN" ] || [ "$ADMIN_TOKEN" = "null" ]; then
  echo "Failed to get admin token"
  exit 1
fi


echo "Got token. Creating user $NEW_USER in realm $REALM..."

# Create user
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://$KEYCLOAK_HOST/admin/realms/$REALM/users" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$NEW_USER\",
    \"firstName\": \"Captain\",
    \"lastName\": \"Cook\",
    \"email\": \"captain@cook.org\",
    \"emailVerified\": true,
    \"enabled\": true,
    \"totp\": false,
    \"attributes\": {
        \"home\": [ \"/home/$NEW_USER\" ]
    },
    \"requiredActions\": [ ],
    \"realmRoles\": [\"default-roles-freva\" ],
    \"credentials\": [{
      \"type\": \"password\",
      \"value\": \"$NEW_PASSWORD\",
      \"temporary\": false
    }]
}")


if [ "$HTTP_STATUS" -ge 300 ]; then
  echo "Failed to create user. HTTP status: $HTTP_STATUS"
  exit 1
fi

echo "Assigning realm-admin role to $NEW_USER for managing the global Flavours"

USER_ID=$(curl -s "http://$KEYCLOAK_HOST/admin/realms/$REALM/users?username=$NEW_USER" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.[0].id')

CLIENT_ID=$(curl -s "http://$KEYCLOAK_HOST/admin/realms/$REALM/clients?clientId=realm-management" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.[0].id')

ROLE_DATA=$(curl -s "http://$KEYCLOAK_HOST/admin/realms/$REALM/clients/$CLIENT_ID/roles/realm-admin" \
  -H "Authorization: Bearer $ADMIN_TOKEN")

curl -s -X POST \
  "http://$KEYCLOAK_HOST/admin/realms/$REALM/users/$USER_ID/role-mappings/clients/$CLIENT_ID" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "[$ROLE_DATA]" > /dev/null

echo -e "\nUser $NEW_USER created with password '$NEW_PASSWORD'"
