-- Memcached interface.
-- Copyright (C) 2005 Evan Martin <martine@danga.com>

{-# LANGUAGE OverloadedStrings, ForeignFunctionInterface #-}

module Network.Memcache.Protocol (
  Server,
  connect,disconnect,stats   -- server-specific commands
) where

-- TODO:
--  - use exceptions where appropriate for protocol errors
--  - expiration time in store

import Network.Memcache
import qualified Network
import Network.Memcache.Key
import Network.Memcache.Serializable
import System.IO
import qualified Data.ByteString.Char8 as B
import Data.ByteString (ByteString)
import Data.Int

import Foreign.C.Types
import Foreign.C.String
import System.IO.Unsafe

foreign import ccall unsafe "stdlib.h atol" c_atol :: CString -> IO CLong
foreign import ccall unsafe "stdlib.h atoll" c_atoll :: CString -> IO CLLong

readInt :: B.ByteString -> Int
readInt = fromEnum . unsafePerformIO . flip B.useAsCString c_atol

readInt64 :: B.ByteString -> Int64
readInt64 = fromInteger . toInteger . unsafePerformIO . flip B.useAsCString c_atoll

-- | Gather results from action until condition is true.
ioUntil :: (a -> Bool) -> IO a -> IO [a]
ioUntil stop io = do
  val <- io
  if stop val then return []
              else do more <- ioUntil stop io
                      return (val:more)

-- | Put out a line with \r\n terminator.
hPutNetLn :: Handle -> String -> IO ()
hPutNetLn h str = hPutStr h (str ++ "\r\n")

-- | Put out a line with \r\n terminator.
hBSPutNetLn :: Handle -> ByteString -> IO ()
hBSPutNetLn h str = B.hPutStr h str >> hPutStr h "\r\n"

-- | Get a line, stripping \r\n terminator.
hGetNetLn :: Handle -> IO [Char]
hGetNetLn h = fmap init (hGetLine h) -- init gets rid of \r

-- | Get a line, stripping \r\n terminator.
hBSGetNetLn :: Handle -> IO ByteString
hBSGetNetLn h = fmap B.init (B.hGetLine h) -- init gets rid of \r

-- | Put out a command (words with terminator) and flush.
hPutCommand :: Handle -> [String] -> IO ()
hPutCommand h strs = hPutNetLn h (unwords strs) >> hFlush h

newtype Server = Server { sHandle :: Handle }

-- connect :: String -> Network.Socket.PortNumber -> IO Server
connect :: Network.HostName -> Network.PortNumber -> IO Server
connect host port = do
  handle <- Network.connectTo host (Network.PortNumber port)
  hSetBuffering handle LineBuffering
  return (Server handle)

disconnect :: Server -> IO ()
disconnect = hClose . sHandle

stats :: Server -> IO [(String, String)]
stats (Server handle) = do
  hPutCommand handle ["stats"]
  statistics <- ioUntil (== "END") (hGetNetLn handle)
  return $ map (tupelize . stripSTAT) statistics where
    stripSTAT ('S':'T':'A':'T':' ':x) = x
    stripSTAT x                       = x
    tupelize line = case words line of
                      (key:rest) -> (key, unwords rest)
                      []         -> (line, "")

store :: (Key k, Serializable s) => String -> Server -> k -> s -> IO Bool
store action (Server handle) key val = do
  let flags = (0::Int)
  let exptime = (0::Int)
  let valstr = serialize val
  let bytes = B.length valstr
  let cmd = unwords [action, toKey key, show flags, show exptime, show bytes]
  hPutNetLn handle cmd
  hBSPutNetLn handle valstr
  hFlush handle
  response <- hGetNetLn handle
  return (response == "STORED")

getOneValue :: Handle -> IO (Maybe ByteString)
getOneValue handle = do
  s <- hBSGetNetLn handle
  case filter (/= " ") $ B.split ' ' s of
    ["VALUE", _, _, sbytes] -> do
      let count = readInt sbytes
      val <- B.hGet handle count
      return $ Just val
    _ -> return Nothing

getOneValueWithId :: Handle -> IO (Maybe (Int64, ByteString))
getOneValueWithId handle = do
  s <- hBSGetNetLn handle
  case filter (/= " ") $ B.split ' ' s of
    ["VALUE", _, _, sbytes, casId] -> do
      let count = readInt sbytes
      val <- B.hGet handle count
      return $ Just (readInt64 casId, val)
    _ -> return Nothing

incDec :: (Key k) => String -> Server -> k -> Int -> IO (Maybe Int)
incDec cmd (Server handle) key delta = do
  hPutCommand handle [cmd, toKey key, show delta]
  response <- hGetNetLn handle
  case response of
    "NOT_FOUND" -> return Nothing
    x           -> return $ Just (read x)


instance Memcache Server where
  set     = store "set"
  add     = store "add"
  replace = store "replace"

  get (Server handle) key = do
    hPutCommand handle ["get", toKey key]
    val <- getOneValue handle
    case val of
      Nothing -> return Nothing
      Just val -> do
        B.hGetLine handle
        B.hGetLine handle
        return $ deserialize val

  gets (Server handle) key = do
    hPutCommand handle ["gets", toKey key]
    val <- getOneValueWithId handle
    case val of
      Nothing -> return Nothing
      Just (casId, val) -> do
        B.hGetLine handle
        B.hGetLine handle
        return $ fmap (\v -> (casId, v)) $ deserialize val

  cas (Server handle) key casId val = do
    let flags = (0::Int)
    let exptime = (0::Int)
    let valstr = serialize val
    let bytes = B.length valstr
    let cmd = unwords ["cas", toKey key, show flags, show exptime, show bytes, 
                       show casId]
    hPutNetLn handle cmd
    hBSPutNetLn handle valstr
    hFlush handle
    response <- hGetNetLn handle
    return (response == "STORED")

  delete (Server handle) key = do
    hPutCommand handle ["delete", toKey key]
    response <- hGetNetLn handle
    return (response == "DELETED")

  incr = incDec "incr"
  decr = incDec "decr"

-- vim: set ts=2 sw=2 et :
