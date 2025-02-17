use crate::pagelist::*;
use crate::platform::Platform;
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::Value as MyValue;

pub type SQLtuple = (String, Vec<MyValue>);

#[async_trait]
pub trait DataSource {
    fn can_run(&self, platform: &Platform) -> bool;
    async fn run(&mut self, platform: &Platform) -> Result<PageList>;
    fn name(&self) -> String;
}
