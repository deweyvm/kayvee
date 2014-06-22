{-# LANGUAGE DoAndIfThenElse #-}
module Database.Kayvee.Kayvee where

import Control.Applicative hiding (empty)
import Control.Exception
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Internal as BS (c2w, w2c)
import Data.Bits
import System.Posix.Types
import System.Posix.Files
import System.Directory
import System.IO
import System.IO.Unsafe
import Data.Hash.MD5
import Data.Binary.Get
import Data.Binary.Put
import Data.Word
import Data.List (genericLength)
import Data.ByteString.Char8 (pack)
import Debug.Trace
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
    if not exists || True
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
    size <- fromIntegral <$> getSize dbPath
    print $ "File size is " ++ show size
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
    traceShow ss $ putWord64be ss
    let ps = pack s
    traceShow ps $ putByteString ps
    let sv = toPointer $ (genericLength v)
    traceShow sv $ putWord64be sv
    let pv = pack v
    traceShow pv $ putByteString pv

readAt :: FilePath
       -> Pointer --position
       -> Pointer --size of data to read
       -> IO BL.ByteString
readAt fp pos size = do
    print $ "reading " ++ show size ++ " bytes starting from " ++ show pos
    h <- openBinaryFile fp ReadMode
    hSeek h AbsoluteSeek (toInteger pos)
    chars <- getChars h (toInteger size)
    hClose h
    return $ (traceShow chars) $ BL.pack (BS.c2w <$> chars)

bsToSize :: BL.ByteString -> Word64
bsToSize bs =
    case (fromInteger . toInteger) <$> BL.unpack bs of
        (b0:b1:b2:b3:b4:b5:b6:b7:[]) ->
            shiftL b0 56
                .|. shiftL b1 48
                .|. shiftL b2 40
                .|. shiftL b3 32
                .|. shiftL b4 24
                .|. shiftL b5 16
                .|. shiftL b6 8
                .|. shiftL b7 0
        xs -> error $ "malformed size " ++ show bs ++ " of length " ++ show (length xs)

readValue :: FilePath -> Pointer -> IO BL.ByteString
readValue fp ptr = do
    size <- bsToSize <$> readAt fp ptr 8
    readAt fp (ptr + 8) size

getChars :: Handle -> Integer -> IO [Char]
getChars h i = do
    helper i
    where helper i = if i == 0
                     then return []
                     else do c <- hGetChar h
                             rest <- helper (i - 1)
                             return $ c : rest

toString :: BL.ByteString -> String
toString x = BS.w2c <$> BL.unpack x

findKey :: FilePath -> String -> [Pointer] -> IO (Maybe Pointer)
findKey fp s (x:xs) = do
    print $ "Going to position " ++ show x
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

