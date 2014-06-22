module Database.Kayvee.Common where

import Prelude hiding (catch)
import Control.Applicative((<$>))
import Control.Exception
import Data.Hash.MD5
import Data.Word
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.List (genericLength)
import Data.ByteString.Char8 (pack)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Internal as BS (c2w, w2c)
import System.Directory
import System.IO
import System.IO.Error hiding (catch)
import System.Posix.Types
import System.Posix.Files
import Debug.Trace

type Hash = (Word32, Word32, Word32, Word32)

data Error = Error String
type Pointer = Word64

hashSize :: Word64
hashSize = 16

ptrSize :: Word64
ptrSize = 8

splitEvery :: Int -> [a] -> [[a]]
splitEvery i xs =
    helper 0 xs [] []
    where helper k (y:ys) acc res =
              let newAcc = y : acc in
              if k == i - 1
              then helper       0 ys        [] (newAcc:res)
              else helper (k + 1) ys (y : acc)        res
          helper _ [] _ res = res

entrySize :: Int
entrySize = 24 --bytes

tup4 :: (Maybe a, Maybe b, Maybe c, Maybe d) -> Maybe (a, b, c, d)
tup4 (Just w, Just x, Just y, Just z) = Just (w, x, y, z)
tup4 _ = Nothing

toWord8List :: String -> [Word8]
toWord8List s = BS.c2w <$> s

readWord :: Get a -> Get (Maybe a)
readWord get' = do
    empty <- isEmpty
    if empty
    then return Nothing
    else do word <- get'
            return $ Just word

readAt :: FilePath
       -> Pointer --position
       -> Word64 --size of data to read
       -> IO BL.ByteString
readAt fp pos size = do
    print $ "reading " ++ show size ++ " bytes starting from " ++ show pos
    h <- openBinaryFile fp ReadMode
    hSeek h AbsoluteSeek (toInteger pos)
    chars <- getChars h (toInteger size)
    hClose h
    return $ (trace ("Read data " ++ show chars)) $ BL.pack (toWord8List chars)

getChars :: Handle -> Integer -> IO [Char]
getChars h i = do
    helper i
    where helper i = if i == 0
                     then return []
                     else do c <- hGetChar h
                             rest <- helper (i - 1)
                             return $ c : rest


createIfNotExists :: FilePath -> IO ()
createIfNotExists fp = do
    exists <- doesFileExist fp
    if not exists || True
    then writeFile fp ""
    else return ()

toString :: BL.ByteString -> String
toString x = BS.w2c <$> BL.unpack x

getSize :: FilePath -> IO FileOffset
getSize path =
    fileSize <$> getFileStatus path

toPointer :: Integer -> Pointer
toPointer = fromInteger

fromPointer :: Pointer -> Integer
fromPointer = toInteger

makeHash :: String -> Hash
makeHash str =
    let (ABCD wds) = md5 (Str str) in
    wds

hashPut :: String -> Pointer -> Put
hashPut s p = do
    let (h0, h1, h2, h3) = makeHash s
    putWord32be h0
    putWord32be h1
    putWord32be h2
    putWord32be h3
    putWord64be p

valuePut :: String -> String -> Put
valuePut s v = do
    let ss = toPointer $ (genericLength s)
    trace ("Putting value: " ++ show ss) $ putWord64be ss
    let ps = pack s
    trace ("Putting value: " ++ show ps) $ putByteString ps
    let sv = toPointer $ (genericLength v)
    trace ("Putting value: " ++ show sv) $ putWord64be sv
    let pv = pack v
    trace ("Putting value: " ++ show pv) $ putByteString pv





listToTuple4 :: Show a => [a] -> (a, a, a, a)
listToTuple4 (w:x:y:z:[]) = (w, x, y, z)
listToTuple4 xs = error $ "Malformed list " ++ show xs

bsToHash :: BL.ByteString -> Hash
bsToHash bs =
    let bytes = (fromInteger . toInteger) <$> BL.unpack bs in
    let words' = splitEvery 4 bytes in
    listToTuple4 $ bytesToWord32 <$> words'
    where bytesToWord32 :: [Word32] -> Word32
          bytesToWord32 (b0:b1:b2:b3:[]) =
              shiftL b0 24
              .|. shiftL b1 16
              .|. shiftL b2 8
              .|. shiftL b3 0
          bytesToWord32 xs = error $ "Malformed bytes " ++ show xs


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

removeIfExists :: FilePath -> IO ()
removeIfExists fileName = removeFile fileName `catch` handleExists
  where handleExists e
          | isDoesNotExistError e = return ()
          | otherwise = throwIO e

moveFile :: FilePath -> FilePath -> IO ()
moveFile x y = renameFile x y
