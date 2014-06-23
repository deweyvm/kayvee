{-# LANGUAGE DoAndIfThenElse #-}
module Database.Kayvee.Kayvee where

import Control.Applicative hiding (empty)
import Control.Exception
import System.IO
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Char8 (pack)
import Debug.Trace

import Database.Kayvee.Common
import Database.Kayvee.Log


schemaPath :: FilePath
schemaPath = "db.schema"

dbPath :: FilePath
dbPath = "db.store"


getOffsets :: Hash -> IO [Pointer]
getOffsets hash = do
    h <- BL.readFile schemaPath
    return $ reverse $ runGet (findOffsets hash) h

findOffsets :: Hash -> Get [Pointer]
findOffsets hash = do
    let read32 = readWord getWord32be
    let read64 = readWord getWord64be
    nextMbs <- (,,,) <$> read32 <*> read32 <*> read32 <*> read32
    ptrMbs <- read64
    let nextm = tup4 nextMbs
    case (ptrMbs, nextm) of
        (Just ptr, Just next) -> if hash == next
                                 then do rest <- findOffsets hash
                                         return $ ptr : rest
                                 else findOffsets hash
        _ -> return []



connect :: IO ()
connect = do
    createIfNotExists dbPath
    createIfNotExists schemaPath

disconnect :: IO ()
disconnect = return ()


put :: String -> String -> IO ()
put s v = do
    size <- fromIntegral <$> getSize dbPath
    logAll $ "File size is " ++ show size
    BL.appendFile schemaPath $ runPut $ hashPut s size
    BL.appendFile dbPath $ runPut $ valuePut s v



readValue :: FilePath -> Pointer -> IO BL.ByteString
readValue fp ptr = do
    size <- bsToSize <$> readAt fp ptr 8
    readAt fp (ptr + 8) size



findKey :: FilePath -> String -> [Pointer] -> IO (Maybe Pointer)
findKey fp s (x:xs) = do
    logAll $ "Going to position " ++ show x
    size <- bsToSize <$> readAt fp x 8
    bs <- readAt fp (x + 8) size
    let string = toString bs
    if string == s
    then return $ Just (x + 8 + size)
    else findKey fp s xs
findKey _ _ [] = return Nothing

get :: String -> IO (Maybe String)
get key = do
    let hash = makeHash key
    offsets <- getOffsets hash
    key <- findKey dbPath key offsets
    case key of
        Just k -> do x <- readValue dbPath k
                     return $ Just $ toString x
        Nothing -> return Nothing
