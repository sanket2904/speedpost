use core::num;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{self, Hash, Hasher};
use std::io::{self, Read};
use std::process::Command;
use std::sync::Arc;
use byteorder::{BigEndian, ByteOrder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use serde::{Serialize, Deserialize};
pub const STARTUP: u8 = b'p';
pub const QUERY: u8 = b'Q';
pub const PARSE: u8 = b'P';
pub const BIND: u8 = b'B';
pub const CLOSE: u8 = b'C';
pub const DESCRIBE: u8 = b'D';
pub const EXECUTE: u8 = b'E';
pub const FLUSH: u8 = b'H';
pub const SYNC: u8 = b'S';
pub const TERMINATE: u8 = b'X';
pub const COPY_DATA: u8 = b'd';
pub const COPY_FAIL: u8 = b'f';
pub const COPY_DONE: u8 = b'c';

pub const AUTHENTICATION: u8 = b'R';
pub const PARAMETER_STATUS: u8 = b'S';
pub const BACKEND_KEY_DATA: u8 = b'K';
pub const PARSE_COMPLETE: u8 = b'1';
pub const BIND_COMPLETE: u8 = b'2';
pub const CLOSE_COMPLETE: u8 = b'3';
pub const PORTAL_SUSPENDED: u8 = b's';
pub const COMMAND_COMPLETE: u8 = b'C';
pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
pub const READY_FOR_QUERY: u8 = b'Z';
pub const ERROR_RESPONSE: u8 = b'E';
pub const NOTICE_RESPONSE: u8 = b'N';
pub const NOTIFICATION_RESPONSE: u8 = b'A';
pub const PARAMETER_DESCRIPTION: u8 = b't';
pub const ROW_DESCRIPTION: u8 = b'T';
pub const DATA_ROW: u8 = b'D';
pub const NO_DATA: u8 = b'n';
// pub const COPY_DATA: u8 = b'd';
// pub const COPY_FAIL: u8 = b'f';
// pub const COPY_DONE: u8 = b'c';
pub const COPY_IN_RESPONSE: u8 = b'G';
pub const COPY_OUT_RESPONSE: u8 = b'H';
pub const COPY_BOTH_RESPONSE: u8 = b'W';


#[derive(Debug)]
struct Query {
    len: i32,
    query: String,
}

impl Query {
    fn decode_query_message(buf: &mut Vec<u8>) -> Query {
        let len = BigEndian::read_i32(&buf[1..5]);
        let (query , _) = get_cstring(&mut buf[5..]).unwrap();
        let query = Query {
            len,
            query,
        };
        query
    }
}


struct CloseComplete {
    Byte1: u8,
    len: i32,
    msg : String,
}


impl CloseComplete {
    fn encode_close_complete(num_rows: usize) -> Vec<u8> {
        let mut buf = vec![];
        buf.push(CLOSE_COMPLETE);
        let main_str = format!("SELECT {}\0", num_rows);
        let mut msg = vec![0; main_str.len()];
        msg.copy_from_slice(main_str.as_bytes());
        let len = 4 + main_str.len() as i32;
        let mut init = vec![0; 4];
        BigEndian::write_i32(&mut init, len);
        buf.extend(init);
        buf.extend(msg);
        buf
    }
}



#[derive(Debug, PartialEq)]
enum Cache {
    CacheHit(State),
    CacheMiss(State),
    NotNeeded,
}
#[derive(Debug , PartialEq)]
enum State {
    Init,
    // ReadyForQueryServer,
    ParseClient(u64),
    DescribeClient(u64),
    SyncClient(u64),
    ParseCompleteServer(u64),
    ParameterDescriptionServer(u64),
    RowDesctioptionServer(u64),
    BindClient(u64),
    BindCompleteServer(u64),
    ExecuteClient(u64),
    DataRowServer(u64),
    CommandCompleteServer(u64),
}

impl Cache {
    async fn next<'a>(&self , message: u8 , mut buffer: Vec<u8> ,  data:  Arc<RwLock<HashMap<u64, DataEntry>>> ,client_read_half: &mut ReadHalf<'a> , server_read_half : &mut ReadHalf<'a>  , client_write_half: &mut WriteHalf<'a> , server_write_half : &mut WriteHalf<'a> ) -> Cache {
        match (self , message) {
            (Cache::CacheMiss(State::Init) , PARSE) => {
                let parser = Parse::decode_parse_message(&mut buffer);
                if !parser.query.contains("SELECT") {
                    write_message(server_write_half, buffer).await.unwrap();
                    return Cache::NotNeeded;
                } 


                let data = data.read().await;
                
                let hash = create_hash(&parser.query);
                let cache = data.get(&hash);
                write_message(server_write_half, buffer).await.unwrap();
                // check if the query contains the word SELECT otherwise dont do anything

                
                match cache {
                    Some(c) => {
                        // send parse complete message to the client
                        let parse = ParseComplete::encode_parse_complete();
                        write_message(client_write_half, parse).await.unwrap();
                        return Cache::CacheHit(State::ParseClient(hash))
                    },
                    None => {
                        // create a new entry in the hashmap
                        return Cache::CacheMiss(State::ParseClient(hash))
                    }
                }
            },

            (Cache::NotNeeded, _) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::NotNeeded;
            },

            (Cache::CacheMiss(State::ParseClient(hash)) , DESCRIBE) => {
                // let describe = Describe::decode_describe_message(&mut buffer);
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::DescribeClient(*hash));
            },
            (Cache::CacheMiss(State::DescribeClient(hash)) , SYNC) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::SyncClient(*hash));
            },
            (Cache::CacheMiss(State::SyncClient(hash)) , PARSE_COMPLETE) => {
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::ParseCompleteServer(*hash));
            },
            (Cache::CacheMiss(State::ParseCompleteServer(hash)) , PARAMETER_DESCRIPTION) => {
                // this needs to be cached
                let mut main = data.write().await;
                let cache = main.entry(*hash).or_insert(DataEntry {
                    data: Vec::new(),
                    parameter_description: Vec::new(),
                    row_description: Vec::new(),
                });
                cache.parameter_description = buffer.clone();
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::ParameterDescriptionServer(*hash));
            },
            (Cache::CacheMiss(State::ParameterDescriptionServer(hash)) , ROW_DESCRIPTION) => {
                let mut main = data.write().await;
                let cache = main.get_mut(&hash).unwrap();
                cache.row_description = buffer.clone();
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::RowDesctioptionServer(*hash));
            },
            (Cache::CacheMiss(State::RowDesctioptionServer(hash)) , BIND) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::BindClient(*hash));
            },
            (Cache::CacheMiss(State::BindClient(hash)) , EXECUTE) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::ExecuteClient(*hash));
            },
            (Cache::CacheMiss(State::BindClient(hash)) , BIND_COMPLETE) => {
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::BindCompleteServer(*hash));
            },
            (Cache::CacheMiss(State::BindCompleteServer(hash)) , EXECUTE) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::ExecuteClient(*hash));
            },
            // client is sending sync message before executing the query
            (Cache::CacheMiss(State::ExecuteClient(hash)) , SYNC) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::SyncClient(*hash));
            },
            (Cache::CacheMiss(State::SyncClient(hash)) , BIND_COMPLETE) => {
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::BindCompleteServer(*hash));
            },
            (Cache::CacheMiss(State::BindCompleteServer(hash)) , DATA_ROW) => {

                let data_row = DataRow::decode_data_row(&mut buffer);
                let mut main = data.write().await;
                let cache = main.get_mut(&hash).unwrap();
                cache.data.push(data_row);

                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::DataRowServer(*hash));
            },
            (Cache::CacheMiss(State::ExecuteClient(hash)) , DATA_ROW) => {
                let data_row = DataRow::decode_data_row(&mut buffer);
                let mut main = data.write().await;
                let cache = main.get_mut(&hash).unwrap();
                cache.data.push(data_row);
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::DataRowServer(*hash));
            },
            (Cache::CacheMiss(State::SyncClient(hash)), DATA_ROW ) => { 
                let data_row = DataRow::decode_data_row(&mut buffer);
                let mut main = data.write().await;
                let cache = main.get_mut(&hash).unwrap();
                cache.data.push(data_row);
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::DataRowServer(*hash));
            },
            (Cache::CacheMiss(State::DataRowServer(hash)) , DATA_ROW) => {
                let data_row = DataRow::decode_data_row(&mut buffer);
                let mut main = data.write().await;
                let cache = main.get_mut(&hash).unwrap();
                cache.data.push(data_row);
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::DataRowServer(*hash));
            },
            (Cache::CacheMiss(State::DataRowServer(hash)) , COMMAND_COMPLETE) => {
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::CommandCompleteServer(*hash));
            },
            (Cache::CacheMiss(State::CommandCompleteServer(_)) , READY_FOR_QUERY) => {
                write_message(client_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::Init);
            },

            (Cache::CacheMiss(State::CommandCompleteServer(_)) , SYNC) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::Init);
            },
            // if the command is close and the cache is miss just send the message to the server but the state could be anything
            (Cache::CacheMiss(_) , CLOSE) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::Init);
            },

            (Cache::CacheMiss(State::Init) , SYNC) => {
                write_message(server_write_half, buffer).await.unwrap();
                return Cache::CacheMiss(State::Init);
            },

            // for all cachemiss init send the message to the server
            // now time to implement cache hit
            (Cache::CacheHit(State::ParseClient(hash)) , DESCRIBE) => {
               
                let data = data.read().await;
                let cache = data.get(&hash).unwrap();
                // send back the parameter description and row description
                write_message(client_write_half, cache.parameter_description.clone()).await.unwrap();
                write_message(client_write_half, cache.row_description.clone()).await.unwrap();
                return Cache::CacheHit(State::DescribeClient(*hash));
            },
            (Cache::CacheHit(State::DescribeClient(hash)) , SYNC) => {
                let ready = ReadyForQuery::encode_ready_for_query();
                write_message(client_write_half, ready).await.unwrap();
                return Cache::CacheHit(State::SyncClient(*hash));
            },
            // manage bind 
            (Cache::CacheHit(State::SyncClient(hash)) , BIND) => {
                let bind_complete = BindComplete::encode_bind_complete();
                write_message(client_write_half, bind_complete).await.unwrap();
                return Cache::CacheHit(State::BindClient(*hash));
            },

            (Cache::CacheHit(State::BindClient(hash)) , EXECUTE) => {
                // send the data row
                let main = data.read().await;
                let cache = main.get(&hash).unwrap();
                for row in &cache.data {
                    let encoded = row.encode_data_row();
                    write_message(client_write_half, encoded).await.unwrap();
                }

                // send the command complete message
                let mut command_complete = CommandComplete::encode_command_complete(cache.data.len());
                let check = get_cstring(&mut command_complete[5..]).unwrap();
                write_message(client_write_half, command_complete).await.unwrap();
                return Cache::CacheHit(State::CommandCompleteServer(*hash));
            },
            (Cache::CacheHit(State::CommandCompleteServer(hash)) , CLOSE) => {
                // send close complete 
               
                let data = data.read().await;
                let cache = data.get(&hash).unwrap();
                let mut close_complete = CloseComplete::encode_close_complete(cache.data.len());
                
                write_message(client_write_half, close_complete).await.unwrap();
                return Cache::CacheHit(State::Init);
            }, 
            (Cache::CacheHit(State::Init) , SYNC) => {
                let ready = ReadyForQuery::encode_ready_for_query();
                write_message(client_write_half, ready).await.unwrap();
                return Cache::CacheHit(State::Init);
            },
            (Cache::CacheHit(State::CommandCompleteServer(hash)), SYNC) => {
                let ready = ReadyForQuery::encode_ready_for_query();
                write_message(client_write_half, ready).await.unwrap();
                return Cache::CacheHit(State::CommandCompleteServer(*hash));
            },
            _ => {
                return Cache::CacheMiss(State::Init);
               
                
            }
        }
    }
    
}


#[derive(Debug, Serialize) ]
struct ParseComplete {
    Byte1: u8,
    len : i32,
}

impl ParseComplete {
    fn encode_parse_complete() -> Vec<u8> {
        let mut buf = vec![0; 5];
        buf[0] = PARSE_COMPLETE;
        BigEndian::write_i32(&mut buf[1..5], 4);
        buf
    }
}



#[derive(Debug, Serialize) ]
struct CommandComplete {
    Byte1: u8,
    len: i32,
    command: String,
}


impl CommandComplete {
    fn encode_command_complete(num_rows: usize) -> Vec<u8> {
        // let mut buf = vec![0; 5];
        // buf[0] = COMMAND_COMPLETE;
        // BigEndian::write_i32(&mut buf[1..5], 4);
        // buf
        let mut buf = vec![];
        buf.push(COMMAND_COMPLETE);
        // let mut init = vec![0; 4]; this will be the length of the message which is going to change
        // BigEndian::write_i32(&mut init, 4);

        let main_str = format!("SELECT {}\0", num_rows);
        let mut msg = vec![0; main_str.len()];
        msg.copy_from_slice(main_str.as_bytes());

        let len = 4 + main_str.len() as i32;
        let mut init = vec![0; 4];
        BigEndian::write_i32(&mut init, len);

        buf.extend(init);
        buf.extend(msg);
        buf

    }
}

#[derive(Debug)]
struct Parse {
    len: i32,
    name: String,
    query: String,
    num_params: i16,
    object_id: i32,
}

#[derive(Debug)]
struct Describe {
    len: i32,
    idk: u8,
    name: String,
}

#[derive(Debug)]
struct Execute {
    len: i32,
    name: String,
    max_rows: i32,
}


#[derive(Debug, Serialize) ]
struct BindComplete {
    Byte1: u8,
    len: i32,
}

impl BindComplete {
    fn encode_bind_complete() -> Vec<u8> {
        let mut buf = vec![0; 5];
        buf[0] = BIND_COMPLETE;
        BigEndian::write_i32(&mut buf[1..5], 4);
        buf
    }
}


impl Execute {
    fn decode_execute_message(buf: &mut Vec<u8>) -> Execute {
        let len = BigEndian::read_i32(&buf[1..5]);
        let (name , position) = get_cstring(&mut buf[5..]).unwrap();
        let position = position + 5;
        let max_rows = BigEndian::read_i32(&buf[position..]);
        let execute = Execute {
            len,
            name,
            max_rows,
        };
        execute
    }
}
#[derive(Debug)]
struct DataRow {
    len: i32,
    num_columns: i16,
    len_col: i32,
    data: Vec<u8>,
}
#[derive(Debug, Serialize) ]
struct ReadyForQuery {
    Byte1: u8,
    len: i32,
    status: char,
}

impl ReadyForQuery {
    fn encode_ready_for_query() -> Vec<u8> {
        let mut buf = vec![0; 6];
        buf[0] = READY_FOR_QUERY;
        BigEndian::write_i32(&mut buf[1..5], 5);
        buf[5] = 'I' as u8;
        buf
    }

    fn decode_ready_for_query(buf: &mut Vec<u8>) -> ReadyForQuery {
        let len = BigEndian::read_i32(&buf[1..5]);
        let status = buf[5] as char;
        let ready_for_query = ReadyForQuery {
            Byte1: READY_FOR_QUERY,
            len,
            status,
        };
        ready_for_query
    }
}


impl DataRow {
    fn decode_data_row(buf: &mut Vec<u8>) -> DataRow {
        let len = BigEndian::read_i32(&buf[1..5]);
        let num_columns = BigEndian::read_i16(&buf[5..7]);
        let len_col = BigEndian::read_i32(&buf[7..11]);
        let data = buf[11..].to_vec();
        let data_row = DataRow {
            len,
            num_columns,
            len_col,
            data,
        };
        data_row
    }
    fn encode_data_row(&self) -> Vec<u8> {
        let mut buf = vec![0; 4];
        BigEndian::write_i32(&mut buf, self.len);
        let mut buf2 = vec![0; 2];
        BigEndian::write_i16(&mut buf2, self.num_columns);
        let mut buf3 = vec![0; 4];
        BigEndian::write_i32(&mut buf3, self.len_col);
        let code = b'D';
        let mut combined = vec![code];
        combined.extend(buf);
        combined.extend(buf2);
        combined.extend(buf3);
        combined.extend(&self.data);
        combined
    }
}

impl Describe {
    fn decode_describe_message(buf: &mut Vec<u8>) -> Describe {
        let len = BigEndian::read_i32(&buf[1..5]);
        let idk = buf[5];
        let (name , _) = get_cstring(&mut buf[6..]).unwrap();
        let describe = Describe {
            len,
            idk,
            name,
        };
        describe
    }

}

impl Parse {
    fn decode_parse_message(buf: &mut Vec<u8>) -> Parse  {
        let len = BigEndian::read_i32(&buf[1..5]);
        let (name , position) = get_cstring(&mut buf[5..]).unwrap();
        let pos = position + 5;
        let (query , _) = get_cstring(&mut buf[pos..]).unwrap();
        // let position_1 = position + pos;
        let last_six_bytes = &buf[buf.len() - 6..];
        let num_params = BigEndian::read_i16(&last_six_bytes[0..2]);
        let object_id = BigEndian::read_i32(&last_six_bytes[2..]);
        let parse = Parse {
            len,
            name,
            query,
            num_params,
            object_id,
        };
        parse
    }
}


struct DataEntry {
    data: Vec<DataRow>,  
    parameter_description: Vec<u8>,
    row_description: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await.unwrap();
    println!("Server listening on port 8080");
    let data: Arc<RwLock<HashMap<u64, DataEntry>>> = Arc::new(RwLock::new(HashMap::new()));
    loop { 
        let (mut client_socket, client_addr) = listener.accept().await.unwrap();
        println!("New client connection: {}", client_addr);
        let data = data.clone();
        tokio::spawn(async move {
            client_socket.set_nodelay(true).unwrap();
            let (mut rx, mut tx) = client_socket.split();
            let mut server = TcpStream::connect("127.0.0.1:5432").await.unwrap();
            server.set_nodelay(true).unwrap();
            let (mut sr, mut st) = server.split();
            let res = read_first_message(&mut rx).await.unwrap();
            let _ = write_message(&mut st, res).await.unwrap();
            let res = read_with_type_server(&mut sr).await.unwrap();
            let _ = write_message(&mut tx, res).await.unwrap();
            let mut state = Cache::CacheMiss(State::Init);
            // let mut cache_state = Cache::CacheMiss(String::from(""));
            loop {
                let _ = tokio::select! {
                    a = read_with_type_client(&mut rx) =>  {
                        match a {
                            Ok(mut a) => {
                                let front_end_message = a[0];
                                
                                match front_end_message {
                                    
                                    QUERY => {
                                        let query = Query::decode_query_message(&mut a);
                                        write_message(&mut st, a).await.unwrap();
                                    },
                                    PARSE => {
                                        state = state.next(PARSE , a , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        // write_message(&mut st, a).await.unwrap();
                                    },
                                    EXECUTE => {
                                        state = state.next(EXECUTE , a , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        // write_message(&mut st, a).await.unwrap();
                                    },
                                    DESCRIBE => {
                                        // let desc = Describe::decode_describe_message(&mut a);
                                        state = state.next(DESCRIBE , a , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        // write_message(&mut st, a).await.unwrap();
                                    },
                                    TERMINATE => {
                                        write_message(&mut st, a).await.unwrap();
                                        break;
                                    },
                                    SYNC => {
                                        
                                        state = state.next(SYNC , a , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        // write_message(&mut st, a).await.unwrap();
                                    },
                                    
                                    BIND => {
                                        // write_message(&mut st, a).await.unwrap();
                                        state = state.next(BIND , a , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        // write_message(&mut st, a).await.unwrap();
                                    },
                                    CLOSE => {
                                        // println!("current state: {:?}", state);
                                        state = state.next(CLOSE , a , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        // write_message(&mut st, a).await.unwrap();
                                    },
                                    _ => {
                                        //println!("Unknown message from client or something we dont care about {:b}",a[0]);
                                        write_message(&mut st, a).await.unwrap();
                                    }
                                }
                            },
                            Err(e) => {
                                if e.kind() == io::ErrorKind::UnexpectedEof {
                                    println!("Client disconnected: {}", client_addr);
                                    break;
                                } else {
                                    println!("Error: {:?}", e);
                                    break;
                                }
                            }
                        }
                    },
                    b = read_with_type_server(&mut sr) => {
                        match b {
                            Ok(mut b) => {
                                let back_end_message = b[0];
                                match back_end_message {
                                    DATA_ROW => {
                                        // write_message(&mut tx, b).await.unwrap();
                                        if state == Cache::NotNeeded {
                                            write_message(&mut tx, b).await.unwrap();
                                        } else {
                                            state = state.next(DATA_ROW , b , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        }
                                    },
                                    COMMAND_COMPLETE => {
                                        // write_message(&mut tx, b).await.unwrap();
                                        
                                        if state == Cache::NotNeeded {
                                            write_message(&mut tx, b).await.unwrap();
                                        } else {
                                            state = state.next(COMMAND_COMPLETE , b , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        }
                                    },
                                    PARSE_COMPLETE => {
                                        // write_message(&mut tx, b).await.unwrap();
                                        if state == Cache::NotNeeded {
                                            write_message(&mut tx, b).await.unwrap();
                                        } else {
                                            state = state.next(PARSE_COMPLETE , b , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        }
                                    },
                                    BIND_COMPLETE => {
                                        // write_message(&mut tx, b).await.unwrap();
                                        if state == Cache::NotNeeded {
                                            write_message(&mut tx, b).await.unwrap();
                                        } else {
                                            state = state.next(BIND_COMPLETE , b , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        }
                                    },
                                    READY_FOR_QUERY => {
                                        // if cache hit is happening dont send ready for query message
                                        write_message(&mut tx, b).await.unwrap();
                                    },
                                    PARAMETER_DESCRIPTION => {
                                        // write_message(&mut tx, b).await.unwrap();
                                        if state == Cache::NotNeeded {
                                            write_message(&mut tx, b).await.unwrap();
                                        } else {
                                            state = state.next(PARAMETER_DESCRIPTION , b , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        }
                                    },
                                    ROW_DESCRIPTION => {

                                        if state == Cache::NotNeeded {
                                            write_message(&mut tx, b).await.unwrap();
                                        } else {
                                            state = state.next(ROW_DESCRIPTION , b , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                        }


                                        // state = state.next(ROW_DESCRIPTION , b , data.clone() , &mut rx , &mut sr , &mut tx , &mut st).await;
                                    },
                                    unknown => {
                                        write_message(&mut tx, b).await.unwrap();
                                       // println!("Unknown message from server or something we dont care about {:b}",unknown);
                                    }
                                }
                            },
                            Err(e) => {
                                if e.kind() == io::ErrorKind::UnexpectedEof {
                                    println!("Client disconnected: {}", client_addr);
                                    break;
                                } else {
                                    println!("Error line 731: {:?}", e);
                                    break;
                                }
                            }
                        }
                    },
                };
            }
            println!("Client disconnected: {}", client_addr);
        });   
    }
}

async fn read_with_type_server<'a>(read_half: &mut tokio::net::tcp::ReadHalf<'a>) -> Result<Vec<u8>, io::Error> {
    let mut buf_type = [0; 1];
    let mut buf_len = [0; 4];
    read_half.read_exact(&mut buf_type).await?;
    read_half.peek(&mut buf_len).await.unwrap();
    let msg_len = BigEndian::read_i32(&buf_len) as usize;
    let mut buf = vec![0; msg_len];
    read_half.read_exact(&mut buf).await.unwrap();
    let combined = [buf_type.to_vec(),  buf].concat();
    Ok(combined)
}
async fn read_with_type_client<'a>(read_half: &mut tokio::net::tcp::ReadHalf<'a>) -> Result<Vec<u8>, io::Error> {
    let mut buf_type = [0; 1];
    let mut buf_len = [0; 4];
    read_half.read_exact(&mut buf_type).await?;
    read_half.peek(&mut buf_len).await.unwrap();
    let msg_len = BigEndian::read_i32(&buf_len) as usize;
    let mut buf = vec![0; msg_len];
    read_half.read_exact(&mut buf).await.unwrap();
    let combined = [buf_type.to_vec(),  buf].concat();
    Ok(combined)
}
async fn read_first_message<'a>(read_half: &mut tokio::net::tcp::ReadHalf<'a>) -> Result<Vec<u8>, io::Error> {
    let mut buf = [0; 4];
    read_half.peek(&mut buf).await.unwrap();
    let msg_len = BigEndian::read_i32(&buf) as usize;
    let mut buf = vec![0; msg_len];
    read_half.read_exact(&mut buf).await.unwrap();  
    // write_half.write_all(&buf).await.unwrap();
    Ok(buf)
}
async fn write_message<'a>(write_half: &mut tokio::net::tcp::WriteHalf<'a>, buf: Vec<u8>) -> Result<(), io::Error> {
    write_half.write_all(&buf).await.unwrap();
    Ok(())
}
fn get_cstring(buf : &mut [u8]) -> Option<(String, usize)> {
    let mut i = 0;
    while i < buf.len() && buf[i] != b'\0' {
        i += 1;
    }
    let string_buf = buf[0..i].to_vec();
    if i == buf.len() {
        None 
    } else {
        Some((String::from_utf8(string_buf).unwrap() , i + 1))
    }
}
fn create_hash(query : &str) -> u64 {
    let mut s = DefaultHasher::new();
    query.hash(&mut s);
    s.finish()
}