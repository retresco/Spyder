# Description of the CrawlUri thrift structure

namespace py spyder.thrift.gen


/**
 * Some typedefs in order to make the code more readable.
 */
typedef i64 timestamp

typedef map<string,string> header

typedef map<string,string> key_value

/**
 * The main strcut for CrawlUris.
 * 
 * This contains some metadata and if possible the saved web page.
 */
struct CrawlUri {
    // the url to crawl
    1: string               url,

    // the host identifier used for queue selection
    2: string               host_identifier,

    // when processing has been started
    3: timestamp            begin_processing,

    // when processing is finished
    4: timestamp            end_processing,

    // the http request headers
    5: header               req_header,

    // the http response headers
    6: header               rep_header

    // the saved content body
    7: string               content_body,

    // additional values from other processors
    8: key_value   optional_vars
}

