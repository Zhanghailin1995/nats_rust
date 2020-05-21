use crate::client::ClientMessageSender;
use crate::error::{NError, Result, ERROR_SUBSCRIPTION_NOT_FOUND};

use std::sync::{Arc};
use tokio::sync::Mutex;
use bitflags::_core::cmp::Ordering;
use std::collections::{HashMap, BTreeSet};


// 为了讲解方便,考虑到Trie的实现以及Cache的实现都是很琐碎,
// 我这里专门实现一个简单的订阅关系查找,不支持*和>这两种模糊匹配.
// 这样就是简单的字符串查找了. 使用map即可.
// 但是为了后续的扩展性呢,我会定义SubListTrait,这样方便后续实现Trie树
#[derive(Debug)]
pub struct Subscription {
    pub msg_sender: Arc<Mutex<ClientMessageSender>>,
    pub subject: String,
    pub queue: Option<String>,
    pub sid: String,
}

//因为孤儿原则,所以必须单独定义ArcSubscription
pub type ArcSubscription = Arc<Subscription>;

impl Subscription {
    pub fn new(
        subject: &str,
        queue: Option<&str>,
        sid: &str,
        msg_sender: Arc<Mutex<ClientMessageSender>>,
    ) -> Self {
        Self {
            subject: subject.to_string(),
            queue: queue.map(|s| s.to_string()),
            sid: sid.to_string(),
            msg_sender,
        }
    }
}

#[derive(Debug, Default)]
pub struct SubResult {
    pub psubs: Vec<ArcSubscription>,// 同一个subject的订阅者集合
    pub qsubs: Vec<Vec<ArcSubscription>>,// 同一个主题下面同一个queue的订阅者集合
}

impl SubResult {
    pub fn new() -> Self {
        Self {
            qsubs: Vec::new(),
            psubs: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.psubs.len() == 0 && self.qsubs.len() == 0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ArcSubscriptionWrapper(pub ArcSubscription);
// PartialEq,Eq,PartialOrd,Ord,找个时间看一下,重要的知识点
impl std::cmp::PartialEq for ArcSubscriptionWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl std::cmp::Eq for ArcSubscriptionWrapper {}

impl std::cmp::PartialOrd for ArcSubscriptionWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for ArcSubscriptionWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.0.as_ref() as *const Subscription as usize;
        let b = other.0.as_ref() as *const Subscription as usize;
        a.cmp(&b)
    }
}

pub type ArcSubResult = Arc<SubResult>;

pub trait SubListTrait {
    fn insert(&mut self, sub: ArcSubscription) -> Result<()>;
    fn remove(&mut self, sub: ArcSubscription) -> Result<()>;
    fn match_subject(&mut self, subject: &str) -> ArcSubResult;
}

#[derive(Debug, Default)]
pub struct SimpleSubList {
    // key subject,value 是订阅者集合
    // 为什么是ArcSubscriptionWrapper,是通过地址来判断唯一性,而不是值,
    // BTreeSet 要求T 实现 std::cmp::Ord
    subs: HashMap<String, BTreeSet<ArcSubscriptionWrapper>>,
    // key subject,value (key queue value 订阅者集合)
    qsubs: HashMap<String, HashMap<String, BTreeSet<ArcSubscriptionWrapper>>>,
}

impl SubListTrait for SimpleSubList {
    fn insert(&mut self, sub: Arc<Subscription>) -> Result<()> {
        if let Some(ref q) = sub.queue {
            // 订阅者带了queue参数
            let entry = self
                .qsubs
                .entry(sub.subject.clone())
                .or_insert(Default::default());
            let queue = entry.entry(q.clone()).or_insert(Default::default());
            queue.insert(ArcSubscriptionWrapper(sub));
        } else {
            // 订阅者没有带queue参数
            let subs = self
                .subs
                .entry(sub.subject.clone()) // 需要转移所有权，所以用Clone 复制了一份
                .or_insert(Default::default());
            subs.insert(ArcSubscriptionWrapper(sub));
        }
        Ok(())
    }

    fn remove(&mut self, sub: ArcSubscription) -> Result<()> {
        if let Some(ref q) = sub.queue {
            if let Some(subs) = self.qsubs.get_mut(&sub.subject) {
                if let Some(qsubs) = subs.get_mut(q) {
                    qsubs.remove(&ArcSubscriptionWrapper(sub.clone()));
                    if qsubs.is_empty() {
                        subs.remove(q);
                    }
                } else {
                    return Err(NError::new(ERROR_SUBSCRIPTION_NOT_FOUND));
                }
                if subs.is_empty() {
                    self.qsubs.remove(&sub.subject);
                }
            } else {
                return Err(NError::new(ERROR_SUBSCRIPTION_NOT_FOUND));
            }
        } else {
            if let Some(subs) = self.subs.get_mut(&sub.subject) {
                subs.remove(&ArcSubscriptionWrapper(sub.clone()));
                if subs.is_empty() {
                    self.subs.remove(&sub.subject);
                }
            }
        }
        Ok(())
    }

    // 当client pub 一个消息的时候，需要来查找所有相关的订阅者
    fn match_subject(&mut self, subject: &str) -> ArcSubResult {
        let mut r = SubResult::default();
        if let Some(subs) = self.subs.get(subject) {
            for s in subs {
                r.psubs.push(s.0.clone());
            }
        }
        if let Some(qsubs) = self.qsubs.get(subject) {
            for (_, qsub) in qsubs {
                let mut v = Vec::with_capacity(qsubs.len());
                for s in qsub {
                    v.push(s.0.clone())
                }
                r.qsubs.push(v);
            }
        }
        Arc::new(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match() {
        let mut sl = SimpleSubList::default();
        //let mut subs = Vec::new();
        let r = sl.match_subject("test");

        assert_eq!(r.psubs.len(), 0);
        assert_eq!(r.qsubs.len(), 0);

    }

}

