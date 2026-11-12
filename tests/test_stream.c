#define _POSIX_C_SOURCE 200809L
#include "rpc.h"
#include <assert.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#define METHOD_READ 31u
#define METHOD_UPLOAD 32u
#define METHOD_EARLY 33u
#define METHOD_ERROR 34u
#define METHOD_IDLE 35u
#define CHUNK (64u * 1024u)
#define TOTAL (9u * 1024u * 1024u + 123u)
typedef struct stream_task {
    rpc_request *request;
    int method;
    struct stream_task *next;
} stream_task;
typedef struct stream_pool {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    stream_task *head, *tail;
    pthread_t workers[4];
    bool stop;
    atomic_int submitted, completed, broken;
} stream_pool;
static void delay_ms(int ms)
{
    struct timespec ts={.tv_sec=ms/1000,.tv_nsec=(ms%1000)*1000000L};
    while(nanosleep(&ts,&ts)!=0){}
}
static void *worker(void *user)
{
    stream_pool *pool=user;
    uint8_t *buf=malloc(CHUNK);
    assert(buf);
    for(;;) {
        pthread_mutex_lock(&pool->mu);
        while(!pool->stop&&!pool->head)pthread_cond_wait(&pool->cv,&pool->mu);
        if(pool->stop&&!pool->head){pthread_mutex_unlock(&pool->mu);break;}
        stream_task *job=pool->head;
        pool->head=job->next;
        if(!pool->head)pool->tail=NULL;
        pthread_mutex_unlock(&pool->mu);
        rpc_request *req=job->request;
        uint32_t status=RPC_STATUS_OK;
        uint64_t response=0;
        size_t response_len=0;
        if(job->method==METHOD_READ) {
            size_t remaining=0;
            assert(rpc_request_size(req)==sizeof(remaining));
            memcpy(&remaining,rpc_request_data(req),sizeof(remaining));
            while(remaining) {
                size_t n=remaining<CHUNK?remaining:CHUNK;
                for(size_t k=0;k<n;++k)buf[k]=(uint8_t)((k+remaining)%251);
                int rc=rpc_request_write(req,buf,n,3000);
                if(rc!=RPC_OK){atomic_fetch_add(&pool->broken,1);
                    status=RPC_STATUS_HANDLER_ERROR;break;}
                remaining-=n;
            }
        } else if(job->method==METHOD_UPLOAD) {
            rpc_buf msg={0};int rc;
            while((rc=rpc_request_read(req,&msg,3000))==RPC_OK) {
                for(size_t k=0;k<msg.len;++k)
                    assert(((uint8_t*)msg.data)[k]==(uint8_t)((response+k)%251));
                response+=msg.len;
            }
            if(rc!=RPC_STREAM_EOF){status=RPC_STATUS_HANDLER_ERROR;
                atomic_fetch_add(&pool->broken,1);}
            else response_len=sizeof(response);
            rpc_buf_free(&msg);
        } else if(job->method==METHOD_IDLE) {
            rpc_buf msg={0};
            int rc=rpc_request_read(req,&msg,3000);
            assert(rc==RPC_ERR_CLOSED||rc==RPC_STREAM_EOF);
            rpc_buf_free(&msg);
        }
        assert(rpc_request_complete(req,status,response_len?&response:NULL,
                                    response_len)==RPC_OK);
        atomic_fetch_add(&pool->completed,1);
        free(job);
    }
    free(buf);
    return NULL;
}
static void pool_init(stream_pool *p)
{
    memset(p,0,sizeof(*p));
    assert(pthread_mutex_init(&p->mu,NULL)==0);
    assert(pthread_cond_init(&p->cv,NULL)==0);
    for(size_t i=0;i<4;++i)assert(pthread_create(&p->workers[i],NULL,worker,p)==0);
}
static void pool_destroy(stream_pool *p)
{
    pthread_mutex_lock(&p->mu);p->stop=true;pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    for(size_t i=0;i<4;++i)pthread_join(p->workers[i],NULL);
    assert(!p->head);
    pthread_cond_destroy(&p->cv);pthread_mutex_destroy(&p->mu);
}
static int submit(rpc_request *req,void *userdata,int method)
{
    stream_pool *p=userdata;
    stream_task *job=calloc(1,sizeof(*job));
    if(!job)return RPC_ERR_NOMEM;
    job->request=req;job->method=method;
    pthread_mutex_lock(&p->mu);
    if(p->tail)p->tail->next=job;else p->head=job;
    p->tail=job;atomic_fetch_add(&p->submitted,1);
    pthread_cond_signal(&p->cv);pthread_mutex_unlock(&p->mu);
    return RPC_PENDING;
}
static int dispatch_read(rpc_request *r,void *u){return submit(r,u,METHOD_READ);}
static int dispatch_upload(rpc_request *r,void *u){return submit(r,u,METHOD_UPLOAD);}
static int dispatch_idle(rpc_request *r,void *u){return submit(r,u,METHOD_IDLE);}
static int early_finish(rpc_request *r,void *u)
{
    (void)u;assert(rpc_request_complete(r,RPC_STATUS_OK,NULL,0)==RPC_OK);
    return RPC_PENDING;
}
static int error_finish(rpc_request *r,void *u)
{(void)r;(void)u;return RPC_ERR_IO;}
static int unary_echo(rpc_request *r,void *u)
{
    (void)u;
    return rpc_request_set_response(r,rpc_request_data(r),rpc_request_size(r))
               ==RPC_OK?RPC_HANDLER_DONE:RPC_ERR_NOMEM;
}
static int register_method(rpc_server *s,uint32_t method,rpc_method_type type,
                           rpc_handler handler,void *userdata)
{
    const rpc_method_desc d={.method=method,.type=type,
                             .handler=handler,.userdata=userdata};
    return rpc_server_register(s,&d);
}
static void *serve(void *p){assert(rpc_server_run(p)==RPC_OK);return NULL;}

static void test_read(uint16_t port,size_t total,int slow)
{
    rpc_client *c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    rpc_stream *s=NULL;
    assert(rpc_stream_open(c,METHOD_READ,RPC_METHOD_SERVER_STREAM,
                           &total,sizeof(total),3000,&s)==RPC_OK);
    rpc_buf bad={0};assert(rpc_call(c,5,"no",2,&bad,100)==RPC_ERR_INVALID);
    rpc_buf_free(&bad);
    rpc_buf msg={0};size_t remaining=total;int rc;
    while((rc=rpc_stream_read(s,&msg,3000))==RPC_OK) {
        size_t n=remaining<CHUNK?remaining:CHUNK;assert(msg.len==n);
        for(size_t k=0;k<n;++k)
            assert(((uint8_t*)msg.data)[k]==(uint8_t)((k+remaining)%251));
        remaining-=n;if(slow)delay_ms(1);
    }
    assert(rc==RPC_STREAM_EOF&&remaining==0);
    assert(rpc_stream_remote_status(s)==RPC_STATUS_OK);
    rpc_stream_destroy(s);rpc_buf_free(&msg);
    rpc_buf resp={0};assert(rpc_call(c,5,"reused",6,&resp,3000)==RPC_OK);
    assert(resp.len==6&&memcmp(resp.data,"reused",6)==0);
    rpc_buf_free(&resp);rpc_client_close(c);
}
static void test_upload(uint16_t port,size_t total)
{
    rpc_client *c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    rpc_stream *s=NULL;
    assert(rpc_stream_open(c,METHOD_UPLOAD,RPC_METHOD_CLIENT_STREAM,
                           NULL,0,3000,&s)==RPC_OK);
    rpc_buf tmp={0};assert(rpc_stream_read(s,&tmp,3000)==RPC_ERR_INVALID);
    uint8_t *buf=malloc(CHUNK);assert(buf);
    for(size_t sent=0;sent<total;) {
        size_t n=total-sent<CHUNK?total-sent:CHUNK;
        for(size_t i=0;i<n;++i)buf[i]=(uint8_t)((sent+i)%251);
        assert(rpc_stream_write(s,buf,n,3000)==RPC_OK);sent+=n;
    }
    assert(rpc_stream_write(s,NULL,0,3000)==RPC_OK);
    assert(rpc_stream_write(s,buf,RPC_MAX_PAYLOAD+1u,3000)==RPC_ERR_LIMIT);
    assert(rpc_stream_finish_send(s,3000)==RPC_OK);
    assert(rpc_stream_write(s,buf,10,3000)==RPC_ERR_INVALID);
    assert(rpc_stream_read(s,&tmp,3000)==RPC_OK);
    uint64_t got=0;assert(tmp.len==sizeof(got));memcpy(&got,tmp.data,sizeof(got));
    assert(got==total);
    assert(rpc_stream_read(s,&tmp,3000)==RPC_STREAM_EOF);
    assert(rpc_stream_read(s,&tmp,3000)==RPC_STREAM_EOF);
    rpc_buf_free(&tmp);free(buf);rpc_stream_destroy(s);
    rpc_buf resp={0};assert(rpc_call(c,5,"ok",2,&resp,3000)==RPC_OK);
    rpc_buf_free(&resp);rpc_client_close(c);
}
static void test_upload_max_frame(uint16_t port)
{
    rpc_client *c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    rpc_stream *s=NULL;
    assert(rpc_stream_open(c,METHOD_UPLOAD,RPC_METHOD_CLIENT_STREAM,
                           NULL,0,3000,&s)==RPC_OK);
    uint8_t *data=malloc(RPC_MAX_PAYLOAD);assert(data);
    for(size_t i=0;i<RPC_MAX_PAYLOAD;++i)data[i]=(uint8_t)(i%251);
    assert(rpc_stream_write(s,data,RPC_MAX_PAYLOAD,3000)==RPC_OK);
    assert(rpc_stream_finish_send(s,3000)==RPC_OK);
    rpc_buf response={0};assert(rpc_stream_read(s,&response,3000)==RPC_OK);
    uint64_t n=0;assert(response.len==sizeof(n));memcpy(&n,response.data,sizeof(n));
    assert(n==RPC_MAX_PAYLOAD);
    assert(rpc_stream_read(s,&response,3000)==RPC_STREAM_EOF);
    rpc_buf_free(&response);free(data);rpc_stream_destroy(s);rpc_client_close(c);
}
static void test_error(uint16_t port)
{
    rpc_client *c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    rpc_stream *s=NULL;rpc_buf data={0};
    assert(rpc_stream_open(c,METHOD_ERROR,RPC_METHOD_SERVER_STREAM,
                           NULL,0,3000,&s)==RPC_OK);
    assert(rpc_stream_read(s,&data,3000)==RPC_ERR_REMOTE);
    assert(rpc_stream_remote_status(s)==RPC_STATUS_HANDLER_ERROR);
    rpc_stream_destroy(s);
    assert(rpc_stream_open(c,999,RPC_METHOD_SERVER_STREAM,NULL,0,3000,&s)==RPC_OK);
    assert(rpc_stream_read(s,&data,3000)==RPC_ERR_REMOTE);
    assert(rpc_stream_remote_status(s)==RPC_STATUS_NO_METHOD);
    rpc_stream_destroy(s);
    assert(rpc_stream_open(c,METHOD_EARLY,RPC_METHOD_SERVER_STREAM,
                           NULL,0,3000,&s)==RPC_OK);
    assert(rpc_stream_read(s,&data,3000)==RPC_STREAM_EOF);
    rpc_stream_destroy(s);rpc_buf_free(&data);rpc_client_close(c);
}
static void test_type_mismatch(uint16_t port)
{
    rpc_client *c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    rpc_buf response={0};
    assert(rpc_call(c,METHOD_READ,NULL,0,&response,3000)==RPC_ERR_REMOTE);
    assert(rpc_client_last_status(c)==RPC_STATUS_METHOD_TYPE_MISMATCH);
    rpc_buf_free(&response);rpc_client_close(c);
    c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    rpc_stream *s=NULL;
    assert(rpc_stream_open(c,5,RPC_METHOD_SERVER_STREAM,NULL,0,3000,&s)==RPC_OK);
    assert(rpc_stream_read(s,&response,3000)==RPC_ERR_REMOTE);
    assert(rpc_stream_remote_status(s)==RPC_STATUS_METHOD_TYPE_MISMATCH);
    rpc_stream_destroy(s);rpc_buf_free(&response);rpc_client_close(c);
}
typedef struct concurrent_job{uint16_t port;int upload;}concurrent_job;
static void *concurrent_stream(void *ptr)
{
    concurrent_job *j=ptr;
    if(j->upload)test_upload(j->port,2u*1024u*1024u+123u);
    else test_read(j->port,2u*1024u*1024u+123u,0);
    return NULL;
}
int main(void)
{
    stream_pool pool;pool_init(&pool);
    rpc_server *srv=rpc_server_create();assert(srv);
    assert(register_method(srv,METHOD_READ,RPC_METHOD_SERVER_STREAM,
                           dispatch_read,&pool)==RPC_OK);
    assert(register_method(srv,METHOD_UPLOAD,RPC_METHOD_CLIENT_STREAM,
                           dispatch_upload,&pool)==RPC_OK);
    assert(register_method(srv,METHOD_IDLE,RPC_METHOD_CLIENT_STREAM,
                           dispatch_idle,&pool)==RPC_OK);
    assert(register_method(srv,METHOD_EARLY,RPC_METHOD_SERVER_STREAM,
                           early_finish,NULL)==RPC_OK);
    assert(register_method(srv,METHOD_ERROR,RPC_METHOD_SERVER_STREAM,
                           error_finish,NULL)==RPC_OK);
    assert(register_method(srv,5,RPC_METHOD_UNARY,unary_echo,NULL)==RPC_OK);
    assert(register_method(srv,METHOD_READ,RPC_METHOD_UNARY,
                           unary_echo,NULL)==RPC_ERR_INVALID);
    assert(register_method(srv,99,RPC_METHOD_BIDI_STREAM,
                           unary_echo,NULL)==RPC_ERR_INVALID);
    assert(rpc_server_listen(srv,"127.0.0.1",0)==RPC_OK);
    uint16_t port=rpc_server_port(srv);pthread_t server;
    assert(pthread_create(&server,NULL,serve,srv)==0);
    test_read(port,0,0);test_read(port,1,0);test_read(port,TOTAL,1);
    test_upload(port,TOTAL);test_upload_max_frame(port);
    test_error(port);test_type_mismatch(port);
    pthread_t concurrent[6];concurrent_job jobs[6];
    for(int i=0;i<6;++i){jobs[i]=(concurrent_job){.port=port,.upload=i%2};
        assert(pthread_create(&concurrent[i],NULL,concurrent_stream,&jobs[i])==0);}
    for(int i=0;i<6;++i)pthread_join(concurrent[i],NULL);
    rpc_client *c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    rpc_stream *s=NULL;size_t big=256u*1024u*1024u;
    int broken=atomic_load(&pool.broken);
    assert(rpc_stream_open(c,METHOD_READ,RPC_METHOD_SERVER_STREAM,
                           &big,sizeof(big),3000,&s)==RPC_OK);
    delay_ms(30);rpc_stream_destroy(s);rpc_client_close(c);
    for(int i=0;i<2000&&atomic_load(&pool.broken)==broken;++i)delay_ms(1);
    assert(atomic_load(&pool.broken)>broken);
    c=rpc_client_connect("127.0.0.1",port,3000);assert(c);
    s=NULL;int submitted=atomic_load(&pool.submitted);
    assert(rpc_stream_open(c,METHOD_IDLE,RPC_METHOD_CLIENT_STREAM,
                           NULL,0,3000,&s)==RPC_OK);
    for(int i=0;i<100&&atomic_load(&pool.submitted)<submitted+1;++i)delay_ms(1);
    rpc_stream_destroy(s);rpc_client_close(c);
    rpc_server_stop(srv);pthread_join(server,NULL);
    assert(atomic_load(&pool.submitted)==atomic_load(&pool.completed));
    pool_destroy(&pool);rpc_server_destroy(srv);
    puts("all streaming integration tests passed");
    return 0;
}
