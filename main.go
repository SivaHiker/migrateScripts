package main


import (
	"io"
	"fmt"
	"os"
	"bufio"
	"database/sql"
	"strings"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

func main() {

	file, err := os.Open("/home/siva/LatestAppOpenUsers_20170512_to_20171107.txt")
	defer file.Close()

	if err != nil {
		println(err)
	}

	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	outputfile, err := os.Create("text.txt")
     if(err!=nil){
     	fmt.Println("Not able to create a file")
	}
	defer outputfile.Close()

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		var userd userDetails
		//fmt.Printf(" > Read %d characters\n", len(line))
		//fmt.Println(line)
		uservalues := strings.Split(line,"+")
		uid := uservalues[0]
        //msisdn := uservalues[1]
        dbConn := getDBConnection()
		dbConn.SetMaxOpenConns(1000)
        defer dbConn.Close()
		err = dbConn.Ping()
		if err != nil {
			fmt.Println(err.Error())
		}
		uid="WcIvzE_log90rBhX"
        fmt.Println("select * from devices where  uid=\""+uid+"\"")
		rows,err := dbConn.Query("select * from devices where  uid=\""+uid+"\"")
		if(err!=nil){
			fmt.Println("Not able to query the uid in the DB -->",uid,err)
		}
		rows.Scan(&userd.Token,&userd.Msisdn,&userd.UID,&userd.AppVersion,&userd.DeviceKey,&userd.DevID,
			   &userd.RegTime,&userd.DevToken,&userd.DevTokenUpdateTs,&userd.DevVersion,&userd.DevType,&userd.Os,
			   	&userd.OsVersion,&userd.UpgradeTime,&userd.LastActivityTime,&userd.AttributeBits,&userd.Sound,&userd.EndTime,
			   		&userd.OriginalAppVersion,&userd.Operator,&userd.Resolution,&userd.Circle,&userd.Pdm)
        //userValues ={userd.Token,userd.Msisdn,userd.UID,userd.AppVersion,userd.DeviceKey,userd.DevID,
			//userd.RegTime,userd.DevToken,userd.DevTokenUpdateTs,userd.DevVersion,userd.DevType,userd.Os,
			//userd.OsVersion,userd.UpgradeTime,userd.LastActivityTime,userd.AttributeBits,userd.Sound,userd.EndTime,
			//userd.OriginalAppVersion,userd.Operator,userd.Resolution,userd.Circle,userd.Pdm}

		msisdn := strconv.FormatInt(int64(userd.Msisdn), 10)
		upgradetime := strconv.FormatInt(int64(userd.UpgradeTime), 10)

		outputfile.WriteString(userd.Token+"::"+msisdn+"::"+userd.Sound+"::"+upgradetime)

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
	DevTokenUpdateTs   int64    `json:"dev_token_update_ts"`
	DevType            string `json:"dev_type"`
	DevVersion         string `json:"dev_version"`
	DeviceKey          string `json:"device_key"`
	EndTime            int64    `json:"end_time"`
	LastActivityTime   int64    `json:"last_activity_time"`
	Msisdn             int64    `json:"msisdn"`
	Operator           string `json:"operator"`
	OriginalAppVersion string `json:"original_app_version"`
	Os                 string `json:"os"`
	OsVersion          string `json:"os_version"`
	Pdm                string `json:"pdm"`
	RegTime            int64    `json:"reg_time"`
	Resolution         string `json:"resolution"`
	Sound              string `json:"sound"`
	Token              string `json:"token"`
	UID                string `json:"uid"`
	UpgradeTime        int64    `json:"upgrade_time"`
}
