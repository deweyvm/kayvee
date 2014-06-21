{-# LANGUAGE DoAndIfThenElse #-}
module Database.Kayvee.Kayvee where

import Control.Applicative hiding (empty)
import qualified Data.ByteString.Lazy as BL
import System.Posix.Types
import System.Posix.Files
import System.Directory
import Data.Hash.MD5
import Data.Binary.Get
import Data.Binary.Put
import Data.Word
import Data.List (genericLength)
import Data.ByteString.Char8 (pack)

type Hash = (Word32, Word32, Word32, Word32)

data Error = Error String
type Pointer = Word64

toPointer :: Integer -> Pointer
toPointer = fromInteger

fromPointer :: Pointer -> Integer
fromPointer = toInteger

makeHash :: String -> Hash
makeHash str =
    let (ABCD wds) = md5 (Str str) in
    wds

schemaPath :: FilePath
schemaPath = "db.schema"

dbPath :: FilePath
dbPath = "db.store"


getOffsets :: Hash -> IO [Pointer]
getOffsets hash = do
    h <- BL.readFile schemaPath
    return $ runGet (findOffsets hash) h

tup4 :: (Maybe a, Maybe b, Maybe c, Maybe d) -> Maybe (a, b, c, d)
tup4 (Just w, Just x, Just y, Just z) = Just (w, x, y, z)
tup4 _ = Nothing

tup2 :: (Maybe a, Maybe b) -> Maybe (a, b)
tup2 = uncurry (liftA2 (,))

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


readWord :: Get a -> Get (Maybe a)
readWord get' = do
    empty <- isEmpty
    if empty
    then return Nothing
    else do word <- get'
            return $ Just word


createIfNotExists :: FilePath -> IO ()
createIfNotExists fp = do
    exists <- doesFileExist fp
    if not exists
    then writeFile fp ""
    else return ()

connect :: IO ()
connect = do

    createIfNotExists dbPath
    createIfNotExists schemaPath


disconnect :: IO ()
disconnect = return ()


getSize :: FilePath -> IO FileOffset
getSize path =
    fileSize <$> getFileStatus path

put :: String -> String -> IO ()
put s v = do
    size <- fromIntegral <$> getSize schemaPath
    BL.appendFile schemaPath $ runPut $ hashPut s size
    BL.appendFile dbPath $ runPut $ rawPut s v

hashPut :: String -> Pointer -> Put
hashPut s p = do
    let (h0, h1, h2, h3) = makeHash s
    putWord32be h0
    putWord32be h1
    putWord32be h2
    putWord32be h3
    putWord64be p



rawPut :: String -> String -> Put
rawPut s v = do
    let ss = toPointer $ (genericLength s)
    putWord64be ss
    let ps = pack s
    putByteString ps
    let sv = toPointer $ (genericLength v)
    putWord64be sv
    let pv = pack v
    putByteString pv




get :: String -> IO (Maybe String)
get = undefined
