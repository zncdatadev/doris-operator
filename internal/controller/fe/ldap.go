package fe

import (
	"context"

	"fmt"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	appsv1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var authenticationLogger = ctrl.Log.WithName("authentication-log")

const LDAP_ADMIN_USER_KEY = "user"

// IsLDAPAuth checks if LDAP authentication is enabled in the provided authentication specifications.
func IsLDAPAuth(
	ctx context.Context,
	client *client.Client,
	authSpec []dorisv1alpha1.AuthenticationSpec) bool {
	for _, auth := range authSpec {
		authClass, err := resolveAuthenticationClass(ctx, client, auth.AuthenticationClass)
		if err != nil {
			authenticationLogger.Error(err, "Failed to resolve AuthenticationClass", "authClassRef", auth.AuthenticationClass)
			return false
		}
		if authClass.Spec.AuthenticationProvider.LDAP != nil {
			authenticationLogger.Info("LDAP authentication is enabled", "authClassRef", auth.AuthenticationClass)
			return true
		}
		authenticationLogger.Info("LDAP authentication is not enabled", "authClassRef", auth.AuthenticationClass)
	}
	return false
}

// resolveAuthenticationClass retrieves the AuthenticationClass object based on the provided reference.
// It logs an error if the retrieval fails and returns nil.
func resolveAuthenticationClass(
	ctx context.Context,
	client *client.Client,
	authClassRef string) (authclass *authv1alpha1.AuthenticationClass, err error) {
	authClassObject := &authv1alpha1.AuthenticationClass{}
	if err = client.GetWithOwnerNamespace(ctx, authClassRef, authClassObject); err != nil {
		authenticationLogger.Error(err, "Failed to get AuthenticationClass", "authClass ref", authClassRef, "namespace", client.GetOwnerNamespace())
		return
	}
	authclass = authClassObject
	return
}

// because can set only one ldap authentication provider in doris cluster, so we only return the first one
func LADPAuth(
	ctx context.Context,
	client *client.Client,
	authSpec []dorisv1alpha1.AuthenticationSpec) []string {
	// get first LDAP authentication provider
	if len(authSpec) == 0 {
		authenticationLogger.Info("No authentication specifications provided")
		return nil
	}
	authClass, err := resolveAuthenticationClass(ctx, client, authSpec[0].AuthenticationClass)
	if err != nil {
		authenticationLogger.Error(err, "Failed to resolve AuthenticationClass", "authClassRef", authSpec[0].AuthenticationClass)
		return nil
	}
	if authClass.Spec.AuthenticationProvider.LDAP == nil {
		authenticationLogger.Info("No LDAP authentication provider found", "authClassRef", authSpec[0].AuthenticationClass)
		return nil
	}
	ldapProvider := authClass.Spec.AuthenticationProvider.LDAP
	if ldapProvider == nil {
		authenticationLogger.Info("LDAP provider is nil", "authClassRef", authSpec[0].AuthenticationClass)
		return nil
	}
	authenticationLogger.Info("LDAP authentication is enabled", "authClassRef", authSpec[0].AuthenticationClass)
	return CreateLDAPConfig(ctx, client, ldapProvider)
}

func CreateLDAPConfig(
	ctx context.Context,
	client *client.Client,
	ldapProvider *authv1alpha1.LDAPProvider) []string {

	ldapAdminUser := GetLDAPAdminUser(ctx, client, ldapProvider.BindCredentials)

	return []string{
		"ldap_host=" + ldapProvider.Hostname,
		"ldap_port=" + fmt.Sprint(ldapProvider.Port),
		"ldap_admin_name=" + ldapAdminUser,
		"ldap_user_basedn=" + ldapProvider.SearchBase,
		"ldap_user_filter=" + ldapProvider.SearchFilter,
		"ldap_group_basedn=" + ldapProvider.SearchBase,
	}
}

// get user from secret
func GetLDAPAdminUser(
	ctx context.Context,
	client *client.Client,
	ldapBindCredentials *commonsv1alpha1.Credentials,
) string {
	if ldapBindCredentials == nil || ldapBindCredentials.SecretClass == "" {
		authenticationLogger.Error(nil, "LDAP bind credentials are not provided")
		return ""
	}
	secret := &appsv1.Secret{}
	err := client.GetWithOwnerNamespace(ctx, ldapBindCredentials.SecretClass, secret)
	if err != nil {
		authenticationLogger.Error(err, "Failed to get LDAP bind credentials secret", "secretClass", ldapBindCredentials.SecretClass, "namespace", client.GetOwnerNamespace())
		return ""
	}
	ldapAdminUser, ok := secret.Data[LDAP_ADMIN_USER_KEY]
	if !ok {
		authenticationLogger.Error(nil, "LDAP admin user not found in secret", "secretClass", ldapBindCredentials.SecretClass, "namespace", client.GetOwnerNamespace(), "key", LDAP_ADMIN_USER_KEY)
		return ""
	}
	return string(ldapAdminUser)
}
