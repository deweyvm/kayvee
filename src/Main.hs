
import Database.Kayvee.Kayvee

main :: IO ()
main = do
    connect
    put "this is the key to the first element" "this is the contents of the first insertion"
    put "apple" "banana"
    x <- get "this is the key to the first element"
    y <- get "apple"
    print x
    print y
    disconnect
