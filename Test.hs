-- Memcached interface.
-- Copyright (C) 2005 Evan Martin <martine@danga.com>

module Main where

import qualified Network.Memcache
import Network.Memcache.Key(hash)
import qualified Network.Memcache.Protocol as S

import Control.Exception
import System.Process
import System.IO   -- used for emulating sleep()
import Test.HUnit

import Data.Int

withServerConnection :: (S.Server -> IO ()) -> IO ()
withServerConnection f = bracket connect disconnect f where
  connect = S.connect "localhost" 11211
  disconnect = S.disconnect

statsTest :: Test
statsTest = TestCase $ withServerConnection $ \server -> do
  stats <- S.stats server
  assertBool "stats returns multiple stats" (length stats > 10)

setGetTest :: Test
setGetTest = TestCase $ withServerConnection $ \server -> do
  let foo = 3 :: Int
  success <- Network.Memcache.set server "foo" foo
  foo' <- Network.Memcache.get server "foo"
  case foo' of
    Nothing -> assertFailure "'foo' not found just after setting it"
    Just v  -> assertEqual "foo value" (3 :: Int) v

deleteTest :: Test
deleteTest = TestCase $ withServerConnection $ \server -> do
  let foo = 3 :: Int
  success <- Network.Memcache.set server "foo2" foo
  success' <- Network.Memcache.delete server "foo2"
  foo' <- Network.Memcache.get server "foo2"
  if (not success')
    then assertFailure "delete did not succeed"
    else case foo' of
    Nothing -> do return ()
    Just v  -> assertEqual "foo value" (3 :: Int) v

getsTest1 :: Test
getsTest1 = TestCase $ withServerConnection $ \server -> do
  let foo = 3 :: Int
  success <- Network.Memcache.set server "foo3" foo
  foo' <- Network.Memcache.gets server "foo3"
  case foo' of
    Nothing -> assertFailure "'foo3' not found just after setting it"
    Just (_, v)  -> assertEqual "foo3 value" (3 :: Int) v

casTest1 :: Test
casTest1 = TestCase $ withServerConnection $ \server -> do
  let foo = 3 :: Int
  success <- Network.Memcache.set server "foo4" foo
  foo' <- Network.Memcache.gets server "foo4"
  case foo' of
    Nothing -> assertFailure "'foo4' not found just after setting it"
    Just (casId, v)  -> do
      assertEqual "foo4 value" (3 :: Int) v
      success' <- Network.Memcache.cas server "foo4" casId foo
      assertBool "cas did not succeed" success'

casTest2 :: Test
casTest2 = TestCase $ withServerConnection $ \server -> do
  let foo = 3 :: Int
  success <- Network.Memcache.set server "foo5" foo
  foo' <- Network.Memcache.gets server "foo5"
  case foo' of
    Nothing -> assertFailure "'foo5' not found just after setting it"
    Just (casId, v)  -> do
      assertEqual "foo5 value" (3 :: Int) v
      success' <- Network.Memcache.cas server "foo5" (casId + 1) foo
      assertBool "cas should not succeed" (not success')

getsTest2 :: Test
getsTest2 = TestCase $ withServerConnection $ \server -> do
  let foo = 3 :: Int
  success <- Network.Memcache.set server "foo6" foo
  foo' <- Network.Memcache.gets server "foo6"
  case foo' of
    Nothing -> assertFailure "'foo6' not found just after setting it"
    Just (casId, v)  -> do
      assertEqual "foo6 value" (3 :: Int) v
      success' <- Network.Memcache.cas server "foo6" casId foo
      assertBool "cas did not succeed" success'
      foo'' <- ((Network.Memcache.gets server "foo6") :: IO (Maybe (Int64, Int)))
      case foo'' of
        Nothing -> assertFailure "'foo6' not found after updating it"
        Just (casId', _) -> 
          assertBool "cas id should be different" (casId /= casId')

hashTest :: Test
hashTest = TestCase $ do
  assertBool "hash produces different values" (hash key1 /= hash key2)
  where key1 = "foo"; key2 = "bar"

-- XXX hack: is there no other way to wait?
sleep :: Int -> IO ()
sleep x = hWaitForInput stdin x >> return ()

main :: IO ()
main = bracket upDaemon downDaemon runTests >> return () where
  upDaemon   = do m <- runCommand "memcached"
                  sleep 200  -- give it time to start up and bind.
                  return m
  downDaemon = terminateProcess
  runTests _ = runTestTT $ TestList [
    statsTest, setGetTest, hashTest, deleteTest,
    getsTest1, casTest1, casTest2, getsTest2]

-- vim: set ts=2 sw=2 et :
