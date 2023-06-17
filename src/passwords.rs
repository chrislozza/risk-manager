use google_secretmanager1::{
    api::{SecretManager, SecretVersion},
    client::{chrono, hyper, hyper_rustls, oauth2, FieldMask},
    Error, Result,
};

pub fn get_password(credentials: &google_cloud_auth::Credentials) -> Result<(), Error> {
    // Load the service account JSON file.
    let service_account_json = std::fs::read_to_string(service_account_json)?;

    // Create a JWT token from the service account JSON file.
    let jwt_token = google_auth::credentials::ServiceAccountCredentials::from_service_account_file(
        "service_account.json",
    )?
    .into_jwt();

    // Create an OAuth2 client using the JWT token.
    let client = oauth2::Client::new(hyper::Client::new(), hyper_rustls::HttpsConnector::new())?;
    client.set_credentials(credentials)?;

    // Create a SecretManager client.
    let secret_manager = SecretManager::new(client, jwt_token);

    // Get the secret version to read.
    let secret_version = secret_manager
        .list_secret_versions("projects/my-project/secrets/my-secret")?
        .versions
        .get(0)
        .ok_or_else(|| Error::new("No secret versions found"))?;

    // Read the secret version.
    let secret = secret_manager.access_secret_version(&secret_version)?;

    // Print the secret value.
    println!("{}", secret.value);

    Ok(())
}
