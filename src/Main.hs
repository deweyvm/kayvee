
import Database.Kayvee.Kayvee

main :: IO ()
main = do
    connect
    put "this" "is a test"
    disconnect
