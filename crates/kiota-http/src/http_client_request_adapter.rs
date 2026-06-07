use std::sync::Arc;

use kiota_abstractions::authentication::AuthenticationProvider;
use kiota_abstractions::serialization::{ParseNode, Parsable, SerializationWriterFactory};
use kiota_abstractions::serialization::registry::{
    PARSE_NODE_FACTORY_REGISTRY, SERIALIZATION_WRITER_FACTORY_REGISTRY,
};
use kiota_abstractions::{
    ErrorMappings, ErasedParsableFactory, KiotaError, RequestAdapter, RequestInformation,
};

use crate::middleware::MiddlewarePipeline;

pub struct HttpClientRequestAdapter {
    auth_provider: Arc<dyn AuthenticationProvider>,
    pipeline: MiddlewarePipeline,
    base_url: String,
}

impl HttpClientRequestAdapter {
    pub fn new(auth_provider: Arc<dyn AuthenticationProvider>) -> Result<Self, KiotaError> {
        let (client, middlewares) = crate::kiota_client_factory::create_default();
        let pipeline = MiddlewarePipeline::new(client, middlewares);

        Ok(Self {
            auth_provider,
            pipeline,
            base_url: String::new(),
        })
    }

    pub fn new_with_client(
        auth_provider: Arc<dyn AuthenticationProvider>,
        client: reqwest::Client,
        middlewares: Vec<Box<dyn crate::middleware::Middleware>>,
    ) -> Result<Self, KiotaError> {
        Ok(Self {
            auth_provider,
            pipeline: MiddlewarePipeline::new(client, middlewares),
            base_url: String::new(),
        })
    }

    async fn get_response(
        &self,
        request_info: &RequestInformation,
    ) -> Result<reqwest::Response, KiotaError> {
        let uri = request_info.get_uri()?;
        let method = match request_info.method {
            kiota_abstractions::HttpMethod::Get => reqwest::Method::GET,
            kiota_abstractions::HttpMethod::Post => reqwest::Method::POST,
            kiota_abstractions::HttpMethod::Patch => reqwest::Method::PATCH,
            kiota_abstractions::HttpMethod::Put => reqwest::Method::PUT,
            kiota_abstractions::HttpMethod::Delete => reqwest::Method::DELETE,
            kiota_abstractions::HttpMethod::Options => reqwest::Method::OPTIONS,
            kiota_abstractions::HttpMethod::Head => reqwest::Method::HEAD,
            kiota_abstractions::HttpMethod::Connect => reqwest::Method::CONNECT,
            kiota_abstractions::HttpMethod::Trace => reqwest::Method::TRACE,
        };

        let mut builder = reqwest::Request::new(method, uri);

        // copy headers
        for (key, values) in request_info.headers.iter() {
            for val in values {
                if let Ok(hv) = val.parse() {
                    builder.headers_mut().append(
                        reqwest::header::HeaderName::from_bytes(key.as_bytes())
                            .map_err(|e| KiotaError::Http(e.to_string()))?,
                        hv,
                    );
                }
            }
        }

        // set body
        if let Some(ref content) = request_info.content {
            *builder.body_mut() = Some(reqwest::Body::from(content.clone()));
        }

        self.pipeline.execute(builder).await
    }

    fn get_root_parse_node(
        &self,
        response_body: &[u8],
        content_type: &str,
    ) -> Result<Option<Box<dyn ParseNode>>, KiotaError> {
        if response_body.is_empty() || content_type.is_empty() {
            return Ok(None);
        }
        // strip content type parameters (e.g., "; charset=utf-8")
        let ct = content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim();
        let node = PARSE_NODE_FACTORY_REGISTRY.get_root_parse_node(ct, response_body)?;
        Ok(Some(node))
    }

    fn throw_if_failed(
        &self,
        status: u16,
        body: &[u8],
        content_type: &str,
        error_mappings: Option<&ErrorMappings>,
    ) -> Result<(), KiotaError> {
        if status < 400 {
            return Ok(());
        }

        let status_str = status.to_string();
        let range_key = if status < 500 { "4XX" } else { "5XX" };

        let factory = error_mappings.and_then(|m| {
            m.get(&status_str)
                .or_else(|| m.get(range_key))
                .or_else(|| m.get("XXX"))
        });

        if let Some(factory) = factory {
            if let Ok(Some(node)) = self.get_root_parse_node(body, content_type) {
                if let Ok(_err_obj) = factory(node.as_ref()) {
                    return Err(KiotaError::Api(kiota_abstractions::ApiError {
                        message: format!("API error {status}"),
                        response_status_code: status as i32,
                        response_headers: kiota_abstractions::ResponseHeaders::new(),
                    }));
                }
            }
        }

        Err(KiotaError::Api(kiota_abstractions::ApiError {
            message: format!("unexpected status code: {status}"),
            response_status_code: status as i32,
            response_headers: kiota_abstractions::ResponseHeaders::new(),
        }))
    }
}

#[async_trait::async_trait]
impl RequestAdapter for HttpClientRequestAdapter {
    async fn send(
        &self,
        request_info: &RequestInformation,
        _factory: &ErasedParsableFactory,
        error_mappings: Option<&ErrorMappings>,
    ) -> Result<Option<Box<dyn Parsable>>, KiotaError> {
        let mut info = clone_request_info(request_info);
        info.path_parameters
            .insert("baseurl".to_string(), self.base_url.clone());

        self.auth_provider
            .authenticate_request(&mut info, None)
            .await?;

        let response = self.get_response(&info).await?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = response
            .bytes()
            .await
            .map_err(|e| KiotaError::Http(e.to_string()))?;

        self.throw_if_failed(status, &body, &content_type, error_mappings)?;

        if status == 204 || body.is_empty() {
            return Ok(None);
        }

        let node = self.get_root_parse_node(&body, &content_type)?;
        match node {
            Some(n) => {
                let mut result = _factory(n.as_ref())?;
                populate_from_node(result.as_mut(), n.as_ref())?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    async fn send_collection(
        &self,
        request_info: &RequestInformation,
        factory: &ErasedParsableFactory,
        error_mappings: Option<&ErrorMappings>,
    ) -> Result<Vec<Box<dyn Parsable>>, KiotaError> {
        let mut info = clone_request_info(request_info);
        info.path_parameters
            .insert("baseurl".to_string(), self.base_url.clone());

        self.auth_provider
            .authenticate_request(&mut info, None)
            .await?;

        let response = self.get_response(&info).await?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = response
            .bytes()
            .await
            .map_err(|e| KiotaError::Http(e.to_string()))?;

        self.throw_if_failed(status, &body, &content_type, error_mappings)?;

        if status == 204 || body.is_empty() {
            return Ok(Vec::new());
        }

        // Parse the JSON array each element is deserialized via the factory
        let root_value: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| KiotaError::Deserialization(e.to_string()))?;

        let mut items = Vec::new();
        match root_value {
            serde_json::Value::Array(arr) => {
                for item_val in arr {
                    let item_bytes = serde_json::to_vec(&item_val)
                        .map_err(|e| KiotaError::Deserialization(e.to_string()))?;
                    if let Some(node) = self.get_root_parse_node(&item_bytes, &content_type)? {
                        let mut parsable = factory(node.as_ref())?;
                        populate_from_node(parsable.as_mut(), node.as_ref())?;
                        items.push(parsable);
                    }
                }
            }
            _ => {
                if let Some(node) = self.get_root_parse_node(&body, &content_type)? {
                    let mut parsable = factory(node.as_ref())?;
                    populate_from_node(parsable.as_mut(), node.as_ref())?;
                    items.push(parsable);
                }
            }
        };

        Ok(items)
    }

    async fn send_no_content(
        &self,
        request_info: &RequestInformation,
        error_mappings: Option<&ErrorMappings>,
    ) -> Result<(), KiotaError> {
        let mut info = clone_request_info(request_info);
        info.path_parameters
            .insert("baseurl".to_string(), self.base_url.clone());

        self.auth_provider
            .authenticate_request(&mut info, None)
            .await?;

        let response = self.get_response(&info).await?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = response
            .bytes()
            .await
            .map_err(|e| KiotaError::Http(e.to_string()))?;

        self.throw_if_failed(status, &body, &content_type, error_mappings)?;
        Ok(())
    }

    async fn send_primitive(
        &self,
        request_info: &RequestInformation,
        error_mappings: Option<&ErrorMappings>,
    ) -> Result<Option<Vec<u8>>, KiotaError> {
        let mut info = clone_request_info(request_info);
        info.path_parameters
            .insert("baseurl".to_string(), self.base_url.clone());

        self.auth_provider
            .authenticate_request(&mut info, None)
            .await?;

        let response = self.get_response(&info).await?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = response
            .bytes()
            .await
            .map_err(|e| KiotaError::Http(e.to_string()))?;

        self.throw_if_failed(status, &body, &content_type, error_mappings)?;

        if status == 204 || body.is_empty() {
            return Ok(None);
        }
        Ok(Some(body.to_vec()))
    }

    fn serialization_writer_factory(&self) -> &dyn SerializationWriterFactory {
        &*SERIALIZATION_WRITER_FACTORY_REGISTRY
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn set_base_url(&mut self, base_url: &str) {
        self.base_url = base_url.to_string();
    }
}

// RequestInformation doesn't implement Clone, so we rebuild it
fn clone_request_info(info: &RequestInformation) -> RequestInformation {
    let mut new_info = RequestInformation::new_with_method_and_url_template(
        info.method,
        &info.url_template,
        info.path_parameters.clone(),
    );
    new_info.query_parameters = info.query_parameters.clone();
    for (key, values) in info.headers.iter() {
        for val in values {
            new_info.headers.add(key, val);
        }
    }
    new_info.content = info.content.clone();
    new_info
}

/// Iterates the parse node's child fields and calls assign_field on the model.
fn populate_from_node(
    model: &mut dyn Parsable,
    node: &dyn ParseNode,
) -> Result<(), KiotaError> {
    let field_names = model.field_names();
    for field in &field_names {
        if let Ok(Some(child)) = node.get_child_node(field) {
            model.assign_field(field, child.as_ref())?;
        }
    }
    Ok(())
}
