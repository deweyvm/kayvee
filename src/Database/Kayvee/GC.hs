{-# LANGUAGE DoAndIfThenElse, NoMonomorphismRestriction #-}
module Database.Kayvee.GC where

import Control.Applicative((<$>))
import Data.ByteString.Char8 (pack)
import qualified Data.ByteString.Lazy as BL
import Data.List (genericLength)
import Data.Word
import Data.Binary.Put

import Database.Kayvee.Kayvee
import Database.Kayvee.Common

tempSchema :: FilePath
tempSchema = "db.schema.temp"

tempDb :: FilePath
tempDb = "db.store.temp"

keyLog :: FilePath
keyLog = "db.keylog.temp"

iterateSchema :: (Hash -> Pointer -> IO ()) -> IO ()
iterateSchema f = do
    let fp = schemaPath
    let cast = fromInteger . toInteger
    totalSize <- cast <$> getSize fp :: IO Pointer
    let numEntries = fromInteger $ (toInteger totalSize) `div` (toInteger entrySize)
    --assert totalSize `rem` schemaEntrySize == 0
    putStrLn $ "Number of entries is " ++ show numEntries
    let helper i = do
            let offset = cast $ (i + 1) * entrySize :: Pointer
            if i > numEntries - 1
            then return ()
            else do hash <- readAt fp (totalSize - offset) hashSize
                    ptr <- readAt fp (totalSize - offset + hashSize) ptrSize
                    f (bsToHash hash) (bsToSize ptr)
                    helper (i + 1)
    helper 0

accumKeys :: Integer -> a -> (String -> a) -> (a -> a -> a) -> IO a
accumKeys maxSize seed f combine = do
    let fp = keyLog
    let helper i s = do
            if i >= fromInteger maxSize
            then return s
            else do size <- bsToSize <$> readAt fp i ptrSize
                    let nextPos = i + size + ptrSize
                    key <- toString <$> readAt fp (i + ptrSize) size
                    let x = f key
                    let c = combine x s
                    helper nextPos c
    helper 0 seed


hasKey :: String -> IO Bool
hasKey key = do
    size <- toInteger <$> getSize keyLog
    accumKeys size False (== key) (||)

-- | Get a key at the given position in the current database
getKey :: Pointer -> IO String
getKey ptr = do
    size <- bsToSize <$> readAt dbPath {-??-} ptr ptrSize
    toString <$> readAt dbPath (ptr + ptrSize) size

addKey :: String -> IO ()
addKey key = do
    let size = genericLength key :: Word64
    BL.appendFile keyLog $ runPut $ helper size key
    where helper :: Word64 -> String -> Put
          helper size str = do
              putWord64be size
              putByteString (pack str)

insertSchema :: String -> Pointer -> IO ()
insertSchema key ptr =
    BL.appendFile tempSchema $ runPut $ hashPut key ptr

insertValue :: String -> String -> IO ()
insertValue key value =
    BL.appendFile tempDb $ runPut $ valuePut key value

processKey :: Hash -> Pointer -> IO ()
processKey hash ptr = do
    putStrLn $ "GC found: " ++ show hash ++ " " ++ show ptr
    key <- getKey ptr
    exists <- hasKey key
    if exists
    then return ()
    else do addKey key
            size <- fromInteger . toInteger <$> getSize tempDb
            insertSchema key size
            valuem <- get key
            case valuem of
                Just value -> insertValue key value
                Nothing -> error $ "Bad key " ++ show key


flipDbs :: IO ()
flipDbs = do
    removeIfExists keyLog
    moveFile tempSchema schemaPath
    moveFile tempDb dbPath

runGc :: IO ()
runGc = do
    createIfNotExists keyLog
    createIfNotExists tempDb
    createIfNotExists tempSchema
    iterateSchema processKey
    flipDbs
