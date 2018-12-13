/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2016, VU University Amsterdam
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

:- module(test_rocksdb, [test_rocksdb/0]).

:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(plunit)).
:- use_module(library(yall)).

:- use_module(library(rocksdb)).

test_rocksdb :-
  run_tests([merge,properties,rocksdb,terms,types]).

:- begin_tests(rocksdb, [cleanup(delete_db)]).

test(basic, Noot==noot) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    {Noot}/[Db]>>(
      rocksdb_put(Db, aap, noot),
      rocksdb_key_value(Db, aap, Noot),
      rocksdb_delete(Db, aap),
      assertion(\+ rocksdb_key(Db, aap))
    )
  ).
test(basic, Noot == noot) :-
  test_db(Dir),
  call_rocksdb(Dir, [Db]>>rocksdb_put(Db, aap, noot)),
  call_rocksdb(
    Dir,
    {Noot}/[Db]>>rocksdb_key_value(Db, aap, Noot),
    [mode(read_only)]
  ).
test(batch, Pairs==[zus-noot]) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    {Pairs}/[Db]>>(
      rocksdb_put(Db, aap, noot),
      rocksdb_key_value(Db, aap, Value),
      rocksdb_batch(Db, [delete(aap),put(zus, Value)]),
      findall(Key-Value, rocksdb_enum(Db, Key, Value), Pairs)
    )
  ).

:- end_tests(rocksdb).

:- begin_tests(terms, [cleanup(delete_db)]).

test(basic, Noot1-Noot2==noot(mies)-noot(1)) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    {Noot1,Noot2}/[Db]>>(
      rocksdb_put(Db, aap, noot(mies)),
      rocksdb_key_value(Db, aap, Noot1),
      rocksdb_put(Db, aap(1), noot(1)),
      rocksdb_key_value(Db, aap(1), Noot2)
    ),
    [key(term),value(term)]
  ).

:- end_tests(terms).

:- begin_tests(types, [cleanup(delete_db)]).

test(int32) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    [Db]>>forall(
            between(-100, 100, I),
            (
              rocksdb_put(Db, key, I),
              assertion(rocksdb_key_value(Db, key, I))
            )
          ),
    [key(atom),value(int32)]
  ).

test(int64) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    [Db]>>forall(
            between(-100, 100, I),
            (
              rocksdb_put(Db, key, I),
              assertion(rocksdb_key_value(Db, key, I))
            )
          ),
    [key(atom),value(int64)]
  ).

test(float) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    [Db]>>forall(
            between(-100, 100, I),
            (
              F is sin(I),
              rocksdb_put(Db, key, F),
              rocksdb_key_value(Db, key, F2),
              assertion(abs(F-F2) < 0.00001)
            )
          ),
    [key(atom),value(float)]
  ).

test(double) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    [Db]>>forall(
            between(-100, 100, I),
            (
              F1 is sin(I),
              rocksdb_put(Db, key, F1),
              rocksdb_key_value(Db, key, F2),
              assertion(F1 =:= F2)
            )
          ),
    [key(atom),value(double)]
  ).

:- end_tests(types).

:- begin_tests(merge, [cleanup(delete_db)]).

test(set, FinalOk==Final) :-
  numlist(1, 100, FinalOk),
  test_db(Dir),
  call_rocksdb(
    Dir,
    {Final}/[Db]>>(
      rocksdb_put(Db, set, []),
      forall(
        between(1, 100, I),
        (
          rocksdb_merge(Db, set, [I]),
          (   I mod 10 =:= 0
          ->  rocksdb_key_value(Db, set, Set),
              assertion(numlist(1, I, Set))
          ;   true
          )
        )
      ),
      rocksdb_key_value(Db, set, Final)
    ),
    [merge(rocksdb_merge_set),value(term)]
  ).
test(new, Final==[1]) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    {Final}/[Db]>>(
      rocksdb_merge(Db, empty, [1]),
      rocksdb_key_value(Db, empty, Final)
    ),
    [merge(rocksdb_merge_set),value(term)]
  ).

:- end_tests(merge).

:- begin_tests(properties, [cleanup(delete_db)]).

test(basic) :-
  test_db(Dir),
  call_rocksdb(
    Dir,
    [Db]>>(
      rocksdb_put(Db, aap, noot(mies)),
      rocksdb_put(Db, aap(1), noot(1)),
      rocksdb_property(Db, estimate_num_keys(Num)),
      assertion(integer(Num))
    ),
    [key(term),value(term)]
  ).

:- end_tests(properties).

test_db('/tmp/test_rocksdb').

delete_db :-
  test_db(Dir),
  delete_db(Dir).

delete_db(Dir) :-
  exists_directory(Dir), !,
  delete_directory_and_contents(Dir).
delete_db(_).
