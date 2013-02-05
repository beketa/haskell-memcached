-- Memcached interface.
-- Copyright (C) 2005 Evan Martin <martine@danga.com>

module Network.Memcache.Key(Key, hash, toKey) where

import Data.List(foldl')
import qualified Data.ByteString.Char8 as B

-- A Memcached key must be hashable (so it can be deterministically distributed
-- across multiple servers) and convertable to a string (as that's what
-- Memcached uses).

class Key a where
  hash     :: a -> Int
  toKey    :: a -> B.ByteString

instance Key B.ByteString where
  -- glib's string hash: fast and good for short strings
  hash  = foldl' (\h i -> 31*h + i) 0 . map fromEnum . B.unpack
  toKey = id

-- vim: set ts=2 sw=2 et :
