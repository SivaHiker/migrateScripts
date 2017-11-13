package main


import (
	"io"
	"fmt"
	"os"
	"bufio"
	"database/sql"
	"strings"
	_ "github.com/go-sql-driver/mysql"
)

func main() {

	file, err := os.Open("/home/siva/LatestAppOpenUsers_20170512_to_20171107.txt")
	defer file.Close()

	if err != nil {
		println(err)
	}

	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		var msidsn int64
		//fmt.Printf(" > Read %d characters\n", len(line))
		//fmt.Println(line)
		uservalues := strings.Split(line,"+")
		uid := uservalues[0]
        //msisdn := uservalues[1]
        dbConn := getDBConnection()
        defer dbConn.Close()
		err = dbConn.Ping()
		if err != nil {
			fmt.Println(err.Error())
		}
		uid="WcIvzE_log90rBhX"
        fmt.Println("select msisdn from devices where  uid=\""+uid+"\"")
		rows,err := dbConn.Query("select * from devices where  uid=\""+uid+"\"")
		if(err!=nil){
			fmt.Println("Not able to query the uid in the DB -->",uid,err)
		}
		rows.Scan(&msidsn)
		fmt.Println(msidsn)
		//fmt.Println(userd.DeviceKey)

	}

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}
}

func getDBConnection() *sql.DB{

	db, err := sql.Open("mysql", "platform:p1atf0rmD1$t@tcp(10.15.0.118:3306)/users")
	if(err!=nil){
		fmt.Println(err)
	}
	return db
}

type userDetails struct {
	AppVersion         string `json:"app_version"`
	AttributeBits      int    `json:"attributeBits"`
	Circle             int    `json:"circle"`
	DevID              string `json:"dev_id"`
	DevToken           string `json:"dev_token"`
	DevTokenUpdateTs   int    `json:"dev_token_update_ts"`
	DevType            string `json:"dev_type"`
	DevVersion         string `json:"dev_version"`
	DeviceKey          string `json:"device_key"`
	EndTime            int    `json:"end_time"`
	LastActivityTime   int    `json:"last_activity_time"`
	Msisdn             int    `json:"msisdn"`
	Operator           string `json:"operator"`
	OriginalAppVersion string `json:"original_app_version"`
	Os                 string `json:"os"`
	OsVersion          string `json:"os_version"`
	Pdm                string `json:"pdm"`
	RegTime            int    `json:"reg_time"`
	Resolution         string `json:"resolution"`
	Sound              string `json:"sound"`
	Token              string `json:"token"`
	UID                string `json:"uid"`
	UpgradeTime        int    `json:"upgrade_time"`
}
