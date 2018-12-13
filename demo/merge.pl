:- use_module(library(apply_macros)).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(ordsets)).

:- use_module(library(rocksdb)).

:- debug(merge).

open :-
	open("test_merge").
open(Db) :-
	rocksdb_open(Db, _, [alias(mdb),open(once),merge(rocksdb_merge_set),value(term)]).

close :-
	rocksdb_close(mdb).

set(Key, Values) :-
	rocksdb_put(mdb, Key, Values).

merge(Key, Values) :-
	rocksdb_merge(mdb, Key, Values).

fetch(Key, Values) :-
	rocksdb_get(mdb, Key, Values).

t(Set) :-
	open,
	set(t1, [1]),
	merge(t1, [2]),
	fetch(t1, Set).

demo(N) :-
	open("test_merge"),
	set(int_set, [1]),
	forall(
    between(1, N, I),
    (
      merge(int_set, [I]),
		  (   I mod 10 =:= 0
		  ->  fetch(int_set, Set),
		      assertion(numlist(1, I, Set))
		  ;   true
		  )
	  )
  ).

demo2(N) :-
	open("test_merge"),
	set(int_set, [1]),
	forall(
    between(1, N, I),
	  (
      fetch(int_set, Set0),
		  ord_add_element(Set0, I, Set1),
		  set(int_set, Set1),
		  (   I mod 10 =:= 0
		  ->  fetch(int_set, Set),
		      assertion(numlist(1, I, Set))
		  ;   true
		  )
	  )
  ).
