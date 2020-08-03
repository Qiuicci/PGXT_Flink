package com.gree.func

import java.net.SocketTimeoutException

import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, RequestIndexer}
import org.apache.flink.util.ExceptionUtils
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException

//es插入失败处理器处理函数
class ActionRequestFailureHandlerImp extends ActionRequestFailureHandler {
   def onFailure(action: ActionRequest, failure: Throwable, restStatusCode: Int, indexer: RequestIndexer) {
     //异常1：ES队列满了，reject异常，放回队列
     if(ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]).isPresent()){
       indexer.add(action)
       //异常2：超时异常timeout，放回队列
     }else if(ExceptionUtils.findThrowable(failure, classOf[SocketTimeoutException]).isPresent()){
       indexer.add(action)
       //异常3：es语法错误，丢弃数据
     }else if(ExceptionUtils.findThrowable(failure,classOf[ElasticsearchParseException]).isPresent()){
       println("sink到ES中语法错误:{}",action.toString())
       //异常4：其他异常，丢弃数据
     }else{
       println("sink到es错误，数据:{},错误类型:{}",action.toString(),org.apache.commons.lang3.exception.ExceptionUtils.getMessage(failure))
     }
  }
}
