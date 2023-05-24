use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::errors::{Errors, Result};
use crate::index;
use crate::options::Options;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

/// BitCask 存储引擎实例结构体
pub struct Engine {
    // 配置项，供用户设置
    options: Arc<Options>,
    // 当前活跃数据文件
    active_file: Arc<RwLock<DataFile>>,
    // 旧的数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    // 数据内存索引
    index: Box<dyn index::Indexer>,
}

impl Engine {
    // 打开bitcask存储引擎实例
    pub fn open(opts: Options) -> Result<Self> {
        // 校验用户传递过来的配置项
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }
        let options = opts.clone();
        // 判断数据目录是否存在
        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = fs::create_dir_all(dir_path) {
                return Err(Errors::FailedToCreateDatabaseDir);
            }
        }
        Ok(Self {
            options: Arc::new(Options {}),
            active_file: Arc::new(()),
            older_files: Arc::new(Default::default()),
            index: Box::new(()),
        })
    }

    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        // 构造 LogRecord
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        // 追加写到活跃数据文件中
        let log_record_pos = self.append_log_record(&mut record)?;
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }
        Ok(())
    }
    // 根据 key 获取对应的数据
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }
        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };
        // 判断 LogRecord 的类型
        if log_record.rec_type == LogRecordType::DELETED {
            return Err(Errors::KeyNotFound);
        }
        Ok(log_record.value.into())
    }

    // 追加数据到当前活跃文件中
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;
        // 获取当前活跃文件
        let mut active_file = self.active_file.write();
        // 判断当前活跃文件是否达到了阈值
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            // 将当前的活跃文件进行持久化
            active_file.sync()?;
            let current_fid = active_file.get_file_id();
            // 旧的数据文件存储到map当中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, old_file);

            // 打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file
        }
        // 执行数据追加写入
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;
        // 根据配置项决定是否持久化
        if self.options.sync_writes {
            active_file.sync()?;
        }
        // 构造内存索引信息
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }
}

fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirPathEmpty);
    }
    if opts.data_file_size <= 0 {
        return Some(Errors::DataFileSizeIllegal);
    }
    None
}
