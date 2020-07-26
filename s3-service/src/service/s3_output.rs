use crate::typedef::Response;

use anyhow::Result;
use hyper::{
    header::{self, HeaderValue},
    Body, StatusCode,
};
use rusoto_s3::*;
use std::convert::TryFrom;
use xml::{
    common::XmlVersion,
    writer::{EventWriter, XmlEvent},
};

pub trait S3Output {
    fn try_into_response(self) -> Result<Response>;
}

impl S3Output for Result<GetObjectOutput> {
    fn try_into_response(self) -> Result<Response> {
        match self {
            Ok(output) => {
                let mut res = Response::new(Body::empty());
                if let Some(body) = output.body {
                    *res.body_mut() = Body::wrap_stream(body);
                }
                if let Some(content_length) = output.content_length {
                    let val = HeaderValue::try_from(format!("{}", content_length))?;
                    res.headers_mut().insert(header::CONTENT_LENGTH, val);
                }
                if let Some(content_type) = output.content_type {
                    let val = HeaderValue::try_from(content_type)?;
                    res.headers_mut().insert(header::CONTENT_TYPE, val);
                }
                // TODO
                Ok(res)
            }
            Err(e) => {
                dbg!(e); // TODO
                let mut res = Response::new(Body::empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(res)
            }
        }
    }
}

impl S3Output for Result<CreateBucketOutput> {
    fn try_into_response(self) -> Result<Response> {
        match self {
            Ok(output) => {
                let mut res = Response::new(Body::empty());
                if let Some(location) = output.location {
                    let val = HeaderValue::try_from(location)?;
                    res.headers_mut().insert(header::LOCATION, val);
                }
                Ok(res)
            }
            Err(e) => {
                dbg!(e);
                let mut res = Response::new(Body::empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(res)
            }
        }
    }
}

impl S3Output for Result<PutObjectOutput> {
    fn try_into_response(self) -> Result<Response> {
        match self {
            Ok(output) => {
                let mut res = Response::new(Body::empty());
                // TODO
                Ok(res)
            }
            Err(e) => {
                dbg!(e); // TODO
                let mut res = Response::new(Body::empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(res)
            }
        }
    }
}

impl S3Output for Result<()> {
    fn try_into_response(self) -> Result<Response> {
        match self {
            Ok(()) => {
                let res = Response::new(Body::empty());
                Ok(res)
            }
            Err(e) => {
                dbg!(e); // TODO
                let mut res = Response::new(Body::empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(res)
            }
        }
    }
}

impl S3Output for Result<DeleteObjectOutput> {
    fn try_into_response(self) -> Result<Response> {
        match self {
            Ok(output) => {
                let res = Response::new(Body::empty());
                Ok(res)
            }
            Err(e) => {
                dbg!(e); // TODO
                let mut res = Response::new(Body::empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(res)
            }
        }
    }
}

impl S3Output for Result<ListBucketsOutput> {
    fn try_into_response(self) -> Result<Response> {
        match self {
            Ok(output) => {
                let mut body = Vec::with_capacity(4096);
                {
                    let mut w = EventWriter::new(&mut body);
                    w.write(XmlEvent::StartDocument {
                        version: XmlVersion::Version10,
                        encoding: Some("UTF-8"),
                        standalone: None,
                    })?;

                    w.write(XmlEvent::start_element("ListBucketsOutput"))?;

                    if let Some(buckets) = output.buckets {
                        w.write(XmlEvent::start_element("Buckets"))?;

                        for bucket in buckets {
                            w.write(XmlEvent::start_element("Bucket"))?;
                            if let Some(creation_date) = bucket.creation_date {
                                w.write(XmlEvent::start_element("CreationDate"))?;
                                w.write(XmlEvent::characters(&creation_date))?;
                                w.write(XmlEvent::end_element())?;
                            }

                            if let Some(name) = bucket.name {
                                w.write(XmlEvent::start_element("Name"))?;
                                w.write(XmlEvent::characters(&name))?;
                                w.write(XmlEvent::end_element())?;
                            }
                            w.write(XmlEvent::end_element())?;
                        }

                        w.write(XmlEvent::end_element())?;
                    }

                    if let Some(owner) = output.owner {
                        w.write(XmlEvent::start_element("Owner"))?;
                        if let Some(display_name) = owner.display_name {
                            w.write(XmlEvent::start_element("DisplayName"))?;
                            w.write(XmlEvent::characters(&display_name))?;
                            w.write(XmlEvent::end_element())?;
                        }
                        if let Some(id) = owner.id {
                            w.write(XmlEvent::start_element("ID"))?;
                            w.write(XmlEvent::characters(&id))?;
                            w.write(XmlEvent::end_element())?;
                        }
                    }

                    w.write(XmlEvent::end_element())?;
                }

                let mut res = Response::new(Body::from(body));
                let val = HeaderValue::try_from(mime::TEXT_XML.as_ref())?;
                res.headers_mut().insert(header::CONTENT_TYPE, val);
                Ok(res)
            }
            Err(e) => {
                dbg!(e); // TODO
                let mut res = Response::new(Body::empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(res)
            }
        }
    }
}

impl S3Output for Result<GetBucketLocationOutput> {
    fn try_into_response(self) -> Result<Response> {
        match self {
            Ok(output) => {
                let mut body = Vec::with_capacity(64);
                let mut w = EventWriter::new(&mut body);
                w.write(XmlEvent::StartDocument {
                    version: XmlVersion::Version10,
                    encoding: Some("UTF-8"),
                    standalone: None,
                })?;

                w.write(XmlEvent::start_element("LocationConstraint"))?;
                if let Some(location_constraint) = output.location_constraint {
                    w.write(XmlEvent::characters(&location_constraint))?;
                }
                w.write(XmlEvent::end_element())?;

                let res = Response::new(Body::from(body));
                Ok(res)
            }
            Err(e) => {
                dbg!(e); // TODO
                let mut res = Response::new(Body::empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(res)
            }
        }
    }
}
