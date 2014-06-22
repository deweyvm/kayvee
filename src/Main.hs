
import Database.Kayvee.Kayvee

main :: IO ()
main = do
    connect
    put "this is the key to the first element" "this is the contents of the first insertion"
    x <- get "this is the key to the first element"
    print x
    disconnect
