use elasticsearch::{
    Elasticsearch,
    auth::Credentials,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};

#[derive(Clone)]
pub struct EsClient {
    host: String,
    username: String,
    password: String,
    es_client: Elasticsearch,
}

impl EsClient {
    pub fn new(host: &str, username: &str, password: &str) -> Self {
        let mut client = EsClient {
            host: host.to_lowercase(),
            username: String::from(username),
            password: String::from(password),
            es_client: Elasticsearch::default(),
        };

        let _ = client.initialize();

        return client;
    }

    pub fn initialize(&mut self) -> std::io::Result<()> {
        // Create Elasticsearch client
        let credentials = Credentials::Basic(self.username.clone(), self.password.clone());

        let conn_pool = SingleNodeConnectionPool::new(self.host.parse().unwrap());
        let transport = match TransportBuilder::new(conn_pool)
            .auth(credentials)
            .cert_validation(elasticsearch::cert::CertificateValidation::None)
            .build()
        {
            Ok(transport) => transport,
            Err(e) => {
                eprintln!("Failed to create Elasticsearch transport: {}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
            }
        };

        let client = Elasticsearch::new(transport);
        self.es_client = client;

        Ok(())
    }

    pub fn get_client(&mut self) -> &Elasticsearch {
        &self.es_client
    }
}
