/**
 *    Tencent is pleased to support the open source community by making CMONGO available.
 *
 *    Copyright (C) 2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *    Licensed under the GNU Affero General Public License Version 3 (the "License");
 *    you may not use this file except in compliance with the License. You may obtain a 
 *    copy of the License at https://www.gnu.org/licenses/agpl-3.0.en.html
 *
 *    Unless required by applicable law or agreed to in writing, software distributed under
 *    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *    either express or implied. See the License for the specific language governing permissions
 *    and limitations under the License.
*/

/* Created by "go tool cgo" - DO NOT EDIT. */

/* package command-line-arguments */

/* Start of preamble from import "C" comments.  */




/* End of preamble from import "C" comments.  */


/* Start of boilerplate cgo prologue.  */

#ifndef GO_CGO_PROLOGUE_H
#define GO_CGO_PROLOGUE_H

typedef signed char GoInt8;
typedef unsigned char GoUint8;
typedef short GoInt16;
typedef unsigned short GoUint16;
typedef int GoInt32;
typedef unsigned int GoUint32;
typedef long long GoInt64;
typedef unsigned long long GoUint64;
typedef GoInt64 GoInt;
typedef GoUint64 GoUint;
typedef __SIZE_TYPE__ GoUintptr;
typedef float GoFloat32;
typedef double GoFloat64;
typedef float _Complex GoComplex64;
typedef double _Complex GoComplex128;

/*
  static assertion to make sure the file is being used on architecture
  at least with matching size of GoInt.
*/
typedef char _check_for_64_bit_pointer_matching_GoInt[sizeof(void*)==64/8 ? 1:-1];

typedef struct { const char *p; GoInt n; } GoString;
typedef void *GoMap;
typedef void *GoChan;
typedef struct { void *t; void *v; } GoInterface;
typedef struct { void *data; GoInt len; GoInt cap; } GoSlice;

#endif

/* End of boilerplate cgo prologue.  */

#ifdef __cplusplus
extern "C" {
#endif


extern void FormatBool(GoUint8 p0, GoSlice* p1, GoUint8* p2);

extern void FormatInt32(GoInt32 p0, GoSlice* p1, GoUint8* p2);

extern void FormatInt8(GoInt8 p0, GoSlice* p1, GoUint8* p2);

extern void FormatInt16(GoInt16 p0, GoSlice* p1, GoUint8* p2);

extern void FormatInt64(GoInt64 p0, GoSlice* p1, GoUint8* p2);

extern void FormatUint32(GoUint32 p0, GoSlice* p1, GoUint8* p2);

extern void FormatUint64(GoUint64 p0, GoSlice* p1, GoUint8* p2);

extern void FormatString(GoString p0, GoSlice* p1, GoUint8* p2);

extern void FormatFloat32(GoFloat32 p0, GoSlice* p1, GoUint8* p2);

extern void FormatFloat64(GoFloat64 p0, GoSlice* p1, GoUint8* p2);

extern void FormatOID(GoString p0, GoSlice* p1, GoUint8* p2);

extern void FormatByteArray(GoString p0, GoSlice* p1, GoUint8* p2);

extern GoUint32 ChunkId(GoString p0);

extern void CreateKeyFile(GoString p0, GoString p1, GoUint8* p2);

extern void RawStringToTableRoute(GoString p0, GoString p1, GoSlice* p2, GoUint8* p3, GoInt64* p4, GoSlice* p5);

extern void GoStyleFork();

extern void TestFormatBsonValues(GoString p0, GoSlice* p1);

#ifdef __cplusplus
}
#endif
