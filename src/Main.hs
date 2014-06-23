
import Database.Kayvee.Kayvee
import Database.Kayvee.GC
main :: IO ()
main = do
    connect
    put "this is the key to the first element" "this is the contents of the first insertion"
    put "apple" "banana"
    put "apple" "fruit"
    x <- get "this is the key to the first element"
    y <- get "apple"
    print x
    print y
    runGc
    disconnect
