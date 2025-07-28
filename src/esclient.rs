use elasticsearch::{
    Elasticsearch,
    auth::Credentials,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};

pub struct EsClient {
    host: String,
    es_client: Elasticsearch,
}

impl EsClient {
    pub fn new(host: &str) -> Self {
        let mut client = EsClient {
            host: host.to_lowercase(),
            es_client: Elasticsearch::default(),
        };

        let _ = client.initialize();

        return client;
    }

    pub fn initialize(&mut self) -> std::io::Result<()> {
        // Create Elasticsearch client
        let credentials = Credentials::Basic(
            String::from("elastic"),
            String::from("GrjXqOPYXAO7gPxlv4P2"),
        );

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
