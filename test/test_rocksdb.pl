:- module(test_rocksdb, [run/0]).

/** <module> RocksDB tests

@author Jan Wielemaker
@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(plunit)).
:- use_module(library(yall)).

:- use_module(library(rocksdb)).

run :-
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
