:- encoding(utf8).
:- module(
  rocksdb,
  [
    call_rocksdb/2,      % +AliasOrDirectory, :Goal_1
    call_rocksdb/3,      % +AliasOrDirectory, :Goal_1, +Options
    rocksdb_batch/2,     % +AliasOrDb, +Actions
    rocksdb_clear/1,     % +AliasOrDirectory
    rocksdb_close/1,     % +AliasOrDb
    rocksdb_delete/2,    % +AliasOrDb, +Key
    rocksdb_key/2,       % +AliasOrDb, ?Key
    rocksdb_key_value/3, % +AliasOrDb, ?Key, ?Value
    rocksdb_merge/3,     % +AliasOrDb, +Key, +Value
    rocksdb_merge_set/5, % +Mode, +Key, +Arg1, +Arg2, -Value
    rocksdb_open/2,      % +Directory, +AliasOrDb
    rocksdb_open/3,      % +Directory, +AliasOrDb, +Options
    rocksdb_property/2,  % +AliasOrDb, ?Property
    rocksdb_put/3,       % +AliasOrDb, +Key, +Value
    rocksdb_size/2,      % +AliasOrDb, -Size
    rocksdb_value/2      % +AliasOrDb, -Value
  ]
).

/** <module> RocksDB interface

RocksDB is an embeddable persistent key-value store for fast storage.
The store can be used only from one process at the same time.  It may
be used from multiple Prolog threads though.  This library provides a
SWI-Prolog binding for RocksDB.  RocksDB just associates byte arrays.
This interface defines several mappings between Prolog datastructures
and byte arrays that may be configured to store both keys and values.
See rocksdb_open/3 for details.

This module defines the following debug flag: `rocksdb'.

---

@author Jan Wielemaker and Wouter Beek
@see https://rocksdb.org/
@version 2017-2019
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(error)).
:- use_module(library(option)).
:- use_module(library(ordsets)).

:- use_module(library(file_ext)).
:- use_module(library(dict)).
:- use_module(library(file_ext)).

:- dynamic
    rocksdb_alias_directory_/2.

:- use_foreign_library(foreign(rocksdb)).

:- meta_predicate
    call_rocksdb(+, 1),
    call_rocksdb(+, 1, :),
    rocksdb_open(+, -, :).





%! call_rocksdb(+AliasOrDirectory:atom, :Goal_1) is det.
%! call_rocksdb(+AliasOrDirectory:atom, :Goal_1, +Options:list(compound)) is det.
%
% Calls Goal_1 on the RocksDB index called AliasOrDirectory.
%
% Options are passed to rocksdb_open/3.

call_rocksdb(AliasOrDir, Goal_1) :-
  call_rocksdb(AliasOrDir, Goal_1, []).


call_rocksdb(AliasOrDir, Goal_1, Options0) :-
  meta_options(is_meta, Options0, Options),
  setup_call_cleanup(
    rocksdb_open(AliasOrDir, Db, Options),
    call(Goal_1, Db),
    rocksdb_close(Db)
  ).



%! rocksdb_batch(+AliasOrDb:or([atom,blob]), +Actions:list(compound)) is det.
%
% Perform a batch of operations on RocksDB as an atomic operation.
% Actions is a list of compound terms of the following forms:
%
%   - delete(+Key:term)
%
%     See rocksdb_delete/2.
%
%   - put(+Key:term,+Value:term)
%
%     See rocksdb_put/3.
%
% Example usage:
%
% ```prolog
% rocksdb_get(Db, some_key, Value),
% rocksdb_batch(Db, [delete(some_key),put(other_key, Value)]),
% ```



%! rocksdb_clear(+AliasOrDirectory:atom) is det.
%
% Assumes that the RocksDB index is already closed.

rocksdb_clear(Alias) :-
  rocksdb_alias_directory_(Alias, Dir), !,
  delete_directory_and_contents(Dir),
  retractall(rocksdb_alias_directory_(Alias,_)).
rocksdb_clear(Dir) :-
  exists_directory(Dir), !,
  delete_directory_and_contents(Dir),
  retractall(rockdb_alias_directory_(_, Dir)).
rocksdb_clear(_).



%! rocksdb_close(+AliasOrDb:or([atom,blob])) is det.
%
% Destroy the RocksDB handle.  Note that anonymous handles are subject
% to (atom) garbage collection.



%! rocksdb_delete(+AliasOrDb:or([atom,blob]), +Key:term) is semidet.
%
% Delete Key from RocksDB.  Fails if Key is not in the database.



%! rocksdb_key(+AliasOrDb:or([atom,blob]), +Key:term) is semidet.
%! rocksdb_key(+AliasOrDb:or([atom,blob]), -Key:term) is nondet.

rocksdb_key(AliasOrDb, Key) :-
  rocksdb_key_value(AliasOrDb, Key, _).



%! rocksdb_key_value(+AliasOrDb:or([atom,blob]), +Key:term, +Value:term) is semidet.
%! rocksdb_key_value(+AliasOrDb:or([atom,blob]), +Key:term, -Value:term) is semidet.
%! rocksdb_key_value(+AliasOrDb:or([atom,blob]), -Key:term, +Value:term) is nondet.
%! rocksdb_key_value(+AliasOrDb:or([atom,blob]), -Key:term, -Value:term) is nondet.
%
% Generalization of rocksdb_enum/3 and rocksdb_get/3.

rocksdb_key_value(AliasOrDb, Key, Value) :-
  ground(Key), !,
  rocksdb_get(AliasOrDb, Key, Value).
rocksdb_key_value(AliasOrDb, Key, Value) :-
  rocksdb_enum(AliasOrDb, Key, Value).



%! rocksdb_merge(+AliasOrDb:or([atom,blob]), +Key:term, +Value:term) is det.
%
% Merge Value with the already existing value for Key.  Requires the
% option merge(:Merger) to be used when opening the database.  Using
% rocksdb_merge/3 rather than rocksdb_get/2, update and rocksdb_put/3
% makes the operation _atomic_ and reduces disk accesses.
%
% `Merger' is called as follows:
%
% ```prolog
% call(:Merger, +How, +Key, +Value0, +MergeValue, -Value)
% ```
%
% Two clauses are required, one for each possible value of `How':
%
%   - `full'
%
%     `MergeValue' is a list of values that need to be merged.
%
%   - `partial'
%
%     `MergeValue' is a single value.
%
% If Key is not in RocksDB, `Value0` is unified with a value that
% depends on the value type.  If the value type is an atom, it is
% unified with the empty atom; if it is `string` or `binary` it is
% unified with an empty string; if it is `int32` or `int64` it is
% unified with the integer 0; and finally if the type is `term` it is
% unified with the empty list.
%
% For example, if the value is a set of Prolog values we open the
% database with value(term) to allow for Prolog lists as value and we
% define merge_set/5 as below.
%
% ```
% merge(partial, _Key, Left, Right, Value) :-
%   ord_union(Left, Right, Value).
% merge(full, _Key, Initial, Additions, Value) :-
%   append([Initial|Additions], List),
%   sort(List, Value).
% ```
%
% If the merge callback fails or raises an exception the merge
% operation fails and the error is logged through the RocksDB logging
% facilities.  Note that the merge callback can be called in a
% different thread or even in a temporary created thread if RocksDB
% decides to merge remaining values in the background.
%
% @error permission_error(merge, rocksdb RocksDB) if the database was
% not opened with the merge(Merger) option.
%
% @see https://github.com/facebook/rocksdb/wiki/Merge-Operator for
% understanding the concept of value merging in RocksDB.



%! rocksdb_open(+Directory:atom, -AliasOrDb:or([atom,blob]), +Options:list(compound)) is det.
%
% Open a RocksDB database in Directory and unify RocksDB with a handle
% to the opened database.  Defined options are:
%
%   - alias(+Name)
%
%     Give the database a name instead of using an anonymous handle.
%     A named database is not subject to GC and must be closed
%     explicitly.
%
%   - open(+How)
%
%     If How is `once' and an alias is given, a second open simply
%     returns a handle to the already open database.
%
%   - key(+Type)
%
%   - merge(:Goal)
%
%     Define RocksDB value merging.  See rocksdb_merge/3.
%
%   - mode(+Mode)
%
%     One of `read_write` (default) or `read_only`.  The latter uses
%     OpenForReadOnly() to open the database.
%
%   - value(+Type)
%
%     Define the type for the key and value. This must be consistent
%     over multiple invocations.  Defined types are:
%
%     - atom
%
%       Accepts an atom or string.  Unifies the result with an atom.
%       Data is stored as a UTF-8 string in RocksDB.
%
%     - string
%
%       Accepts an atom or string.  Unifies the result with a string.
%       Data is stored as a UTF-8 string in RocksDB.
%
%     - binary
%
%       Accepts an atom or string with codes in the range 0…255.
%       Unifies the result with a string. Data is stored as a sequence
%       of bytes in RocksDB.
%
%     - int32
%
%       Maps to a Prolog integer in the range
%       -2,147,483,648…2,147,483,647, stored as a 4 bytes in native
%       byte order.
%
%     - int64
%
%       Maps to a Prolog integer in the range
%       -9,223,372,036,854,775,808…9,223,372,036,854,775,807, stored
%       as a 8 bytes in native byte order.
%
%     - float
%
%       Value is mapped to a 32-bit floating point number.
%
%     - double
%
%       Value is mapped to a 34-bit floating point number (double).
%
%     - term
%
%       Stores any Prolog term.  Stored using PL_record_external().
%       The PL_record_external() function serializes the internal
%       structure of a term, including _cycles_, _sharing_ and
%       _attributes_.  This means that if the key is a term, it only
%       matches if the the same cycles and sharing is used.  For
%       example, `X = f(a), Key = k(X,X)` is a different key from `Key
%       = k(f(a),f(a))` and `X = [a|X]` is a different key from `X =
%       [a,a|X]`.  Applications for which such keys should match must
%       first normalize the key.  Normalization can be based on
%       term_factorized/3 from library(terms).
%
% If Directory does not yet exist it is created.
%
% If option alias/1 is not given, the last non-empty subpath of
% Directory is used as the alias.

rocksdb_open(Dir, Db) :-
  rocksdb_open(Dir, Db, []).


rocksdb_open(Dir, Db, Options0) :-
  meta_options(is_meta, Options0, Options1),
  rocksdb_create_directory_(Dir, Options1),
  set_alias_option_(Dir, Options1, Alias, Options2),
  store_alias_directory_(Alias, Dir),
  rocksdb_open_(Dir, Db, Options2).

rocksdb_create_directory_(_, Options) :-
  option(mode(read_only), Options), !.
rocksdb_create_directory_(Dir, _) :-
  create_directory(Dir).

store_alias_directory_(Alias, Dir) :-
  rocksdb_alias_directory_(Alias, Dir), !.
% Alias already exists: throw exception.
store_alias_directory_(Alias, _) :-
  rocksdb_alias_directory_(Alias, _),
  throw(error(already_exists(rocksbd_alias,Alias),rocksdb_open/3)).
store_alias_directory_(Alias, Dir) :-
  assertz(rocksdb_alias_directory_(Alias,Dir)).

set_alias_option_(_, Options, Alias, Options) :-
  option(alias(Alias), Options), !.
set_alias_option_(Dir, Options1, Alias, Options2) :-
  directory_subdirectories(Dir, L),
  reverse(L, Rev),
  member(Alias, Rev),
  Alias \== '',
  \+ is_dummy_file(Alias), !,
  merge_options([alias(Alias)], Options1, Options2).

is_meta(merge).



%! rocksdb_property(+AliasOrDb, +Property:compound) is semidet.
%! rocksdb_property(+AliasOrDb, -Property:compound) is multi.

rocksdb_property(AliasOrDb, Property) :-
  var(Property), !,
  rocksdb_property_(Key),
  rocksdb_property(AliasOrDb, Key, Value),
  compound_name_arguments(Property, Key, [Value]).
rocksdb_property(AliasOrDb, Property) :-
  compound_name_arguments(Property, Key, [Value]), !,
  rocksdb_property(AliasOrDb, Key, Value).
rocksdb_property(_AliasOrDb, Property) :-
  type_error(rocksdb_property, Property).

rocksdb_property_(estimate_num_keys).



%! rocksdb_put(+AliasOrDb:or([atom,blob]), +Key:term, +Value:term) is det.
%
% Add Key-Value to the RocksDB  database.   If  Key  already has a
% value, the existing value is silently replaced by Value.



%! rocksdb_size(+AliasOrDb:or([atom,blob]), -Size:nonneg) is det.
%
% The Size of a RocksDB index is the number of unique pairs that are
% stored in it.

rocksdb_size(AliasOrDb, Size) :-
  aggregate_all(count, rocksdb_enum(AliasOrDb, _, _), Size).



%! rocksdb_value(+AliasOrDb:or([atom,blob]), -Value:term) is nondet.

rocksdb_value(AliasOrDb, Value) :-
  rocksdb_key_value(AliasOrDb, _, Value).





% MERGE OPERATORS %

%! rocksdb_merge_set(+Mode:oneof([full,partial]), +Key:term, +Arg1:list(term), +Arg2:or([list(term),term]), -Value:list(term)) is det.

rocksdb_merge_set(partial, _Key, L1, L2, Value) :-
  ord_union(L1, L2, Value),
  (debugging(rocksdb) -> debug_merge_set_([L1,L2]) ; true).
rocksdb_merge_set(full, _Key, L, Ls, Value) :-
  ord_union([L|Ls], Value),
  (debugging(rocksdb) -> debug_merge_set_([L|Ls]) ; true).

debug_merge_set_(Sets) :-
  maplist(length, Sets, Cardinalities),
  debug(rocksdb, "Set merge: ~p", [Cardinalities]).
