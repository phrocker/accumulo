/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#include <stdint.h>
#include <string.h>
#include <jni.h>
#include <string>
#include <string.h>
#include <iostream>
#include <vector>
#include <stdlib.h>
#include "BlockAllocator.h"

#ifndef __FIELD__
#define __FIELD__

class Field {
private:
  BlockSegment segment_;
  public:
  uint8_t *data_;
  size_t size_;

  int compare(const uint8_t *d1, int len1, const uint8_t *d2, int len2) const{
    int result = memcmp(d1, d2, len1 < len2 ? len1 : len2);

    if(result != 0)
      return result;
    if(len1 == len2)
      return 0;
    if(len1 < len2)
      return -1;

    return 1;
  }

  Field(){
    size_ = 131072;

          segment_ =  BlockAllocator::getAllocator()->allocate( size_ ) ;

          data_ = segment_.get();
  }


  Field(const Field  &other) = delete;
      Field(Field &&other) = default;
      Field &operator =(Field &&other) = default;
      Field operator =(const Field &other) = delete;

    explicit Field(JNIEnv *env, jbyteArray f, int length) : size_(length){
        segment_ = BlockAllocator::getAllocator()->allocate( length );
        size_ = length;
        data_ = segment_.get();
        env->GetByteArrayRegion(f, 0, size_, reinterpret_cast<jbyte *>(data_));
    }

  explicit Field(JNIEnv *env, jbyteArray f){
        auto length  = env->GetArrayLength(f);
        segment_ = BlockAllocator::getAllocator()->allocate( length );
        size_ = length;

        data_ = segment_.get();

        env->GetByteArrayRegion(f, 0, size_, reinterpret_cast<jbyte *>(data_));
  }

  explicit Field(uint8_t *f, int32_t length) : size_(length){
    // don't make an assumption about the lifetime of f
    segment_ = BlockAllocator::getAllocator()->allocate( length );
    size_ = length;

    data_ = segment_.get();

    memcpy(data_, f, size_);
  }

  explicit Field(const char * const cstr){
        //constructor for testing C++
        auto length = strlen(cstr);
        segment_ = BlockAllocator::getAllocator()->allocate( length );
        size_ = length;
        data_ = segment_.get();
        memcpy(data_, cstr, size_);
    }


    virtual ~Field(){
      // returning the block segment is optional, but preferred.
      BlockAllocator::getAllocator()->deallocate(std::move(segment_));
    }


  void set(const char *d, int l){
    if(l < 0 || l > size_){
      std::cerr << "Tried to set field with value that is too long " << l << " " << size_ << std::endl;
    }
    memcpy(data_, d, l);
  }

  void set(JNIEnv *env, jbyteArray f, int l){
    if(l < 0 || l > size_){
      std::cerr << "Tried to set field with value that is too long " << l << " " << size_ << std::endl;
    }
    if (l > size_ && l > segment_.size()){
        BlockAllocator::getAllocator()->deallocate(std::move(segment_));
        segment_ = BlockAllocator::getAllocator()->allocate( l );
        size_ = l;
        data_ = segment_.get();
    }
    env->GetByteArrayRegion(f, 0, size_, reinterpret_cast<jbyte *>(data_));
  }

  int compare(const Field &of) const{
    return compare(data_, size_, of.data_, of.size_);
  }

  bool operator<(const Field &of) const{
    return compare(of) < 0;
  }

  int32_t length() const {
    return size_;
  }

  void fillIn(JNIEnv *env, jbyteArray d) const {
    //TODO ensure lengths match up
    env->SetByteArrayRegion(d, 0, size_, reinterpret_cast<jbyte *>(data_));
  }

  jbyteArray createJByteArray(JNIEnv *env) const{
    jbyteArray valData = env->NewByteArray(size_);
    env->SetByteArrayRegion(valData, 0, size_, reinterpret_cast<jbyte *>(data_));
    return valData;
  }

  std::string toString() const{
    return std::string(reinterpret_cast<char *>(data_), size_);
  }

  void clear(){

    //delete(field);
  }


};

struct LocalField : public Field {
  explicit LocalField(JNIEnv *env, jbyteArray f) : Field(env,f){
  }

  ~LocalField(){

  }
};
#endif
