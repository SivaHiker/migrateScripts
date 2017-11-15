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
	"encoding/csv"
	"time"
)

func main() {

	file, err := os.Open("/home/siva/LatestAppOpenUsers_20170512_to_20171107.txt")
	defer file.Close()

	if err != nil {
		println(err)
	}

	dbConn := getDBConnection()
	dbConn.SetMaxOpenConns(10000)

	defer dbConn.Close()
	err = dbConn.Ping()
	if err != nil {
		fmt.Println(err.Error())
	}
	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	outputfile, err := os.Create("text.txt")
     if(err!=nil){
     	fmt.Println("Not able to create a file")
	}
	defer outputfile.Close()

	csvfile, err := os.Create("result.csv")
	if(err!=nil){
		fmt.Println("Not able to create a csv file")
	}

	writer := csv.NewWriter(csvfile)
	defer writer.Flush()
	defer csvfile.Close()
	limiter := time.Tick(time.Nanosecond * 1000000)

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


		uid="WcIvzE_log90rBhX"
        fmt.Println("select * from devices where  uid=\""+uid+"\"")
        <-limiter
		stmt, err := dbConn.Prepare("select * from devices where  uid=\""+uid+"\"")
		if err != nil {
			fmt.Println(err)
		}
		defer stmt.Close()
		rows, err := stmt.Query()
		if err != nil {
			fmt.Println(err)
		}
		defer rows.Close()
		//rows,err := dbConn.Query("select * from devices where  uid=\""+uid+"\"")
		//if(err!=nil){
		//	fmt.Println("Not able to query the uid in the DB -->",uid,err)
		//}

		for rows.Next() {
			err := rows.Scan(&userd.Token, &userd.Msisdn, &userd.UID, &userd.AppVersion, &userd.DeviceKey, &userd.DevID,
				&userd.RegTime, &userd.DevToken, &userd.DevTokenUpdateTs, &userd.DevVersion, &userd.DevType, &userd.Os,
				&userd.OsVersion, &userd.UpgradeTime, &userd.LastActivityTime, &userd.AttributeBits, &userd.Sound, &userd.EndTime,
				&userd.OriginalAppVersion, &userd.Operator, &userd.Resolution, &userd.Circle, &userd.Pdm)
			fmt.Println(err)
		}
        //userValues ={userd.Token,userd.Msisdn,userd.UID,userd.AppVersion,userd.DeviceKey,userd.DevID,
			//userd.RegTime,userd.DevToken,userd.DevTokenUpdateTs,userd.DevVersion,userd.DevType,userd.Os,
			//userd.OsVersion,userd.UpgradeTime,userd.LastActivityTime,userd.AttributeBits,userd.Sound,userd.EndTime,
			//userd.OriginalAppVersion,userd.Operator,userd.Resolution,userd.Circle,userd.Pdm}

		//outputfile.WriteString(ToString(userd.Token)+"::+"+ToIntegerVal(userd.Msisdn)+"::"+ToString(userd.
		//	Sound)+"::"+ToIntegerVal(userd.UpgradeTime)+"\n")



		msisdnReqd := ToIntegerVal(userd.Msisdn)
		if strings.HasPrefix(msisdnReqd,"9") {
			msisdnReqd=strings.Replace(msisdnReqd,"9","1",1)
		} else if (strings.HasPrefix(msisdnReqd,"9")) {
			msisdnReqd=strings.Replace(msisdnReqd,"8","2",1)
		} else if (strings.HasPrefix(msisdnReqd,"9")) {
			msisdnReqd=strings.Replace(msisdnReqd,"7","3",1)
		} else {
			continue
		}

		outputfile.WriteString(ToString(userd.Token)+"::"+msisdnReqd+"::"+ToString(userd.UID)+"::"+
			ToString(userd.AppVersion)+"::"+ToString(userd.DeviceKey)+"::"+ToString(userd.DevID)+"::"+ToIntegerVal(userd.
			RegTime)+"::"+ToString(userd.DevToken)+"::"+ToIntegerVal(userd.DevTokenUpdateTs)+"::"+ToString(userd.
			DevVersion)+"::"+ ToString(userd.DevType)+"::"+ToString(userd.Os)+"::"+ToString(userd.OsVersion)+"::"+
			ToIntegerVal(userd.UpgradeTime)+"::"+ToIntegerVal(userd.LastActivityTime)+"::"+ToStringFromInt(userd.
			AttributeBits)+"::"+ToString(userd.Sound)+"::"+ToIntegerVal(userd.EndTime)+"::"+ ToString(userd.
			OriginalAppVersion)+"::"+userd.Operator+"::"+userd.Resolution+"::"+ToStringFromInt(userd.Circle)+"::"+userd.
			Pdm+"\n")

		records := [][]string{
			{ToString(userd.Token),msisdnReqd,ToString(userd.UID),
				ToString(userd.AppVersion),ToString(userd.DeviceKey),ToString(userd.DevID),ToIntegerVal(userd.
				RegTime),ToString(userd.DevToken),ToIntegerVal(userd.DevTokenUpdateTs),ToString(userd.
				DevVersion), ToString(userd.DevType),ToString(userd.Os),ToString(userd.OsVersion),
				ToIntegerVal(userd.UpgradeTime),ToIntegerVal(userd.LastActivityTime),ToStringFromInt(userd.
				AttributeBits),ToString(userd.Sound),ToIntegerVal(userd.EndTime), ToString(userd.
				OriginalAppVersion),userd.Operator,userd.Resolution,ToStringFromInt(userd.Circle),userd.
				Pdm},
		}
		for _, value := range records {
			err := writer.Write(value)
			if(err!=nil){
				fmt.Println(err.Error())
				fmt.Println("Not able to write the records into csv file")
			}
		}
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

func ToNullString(s string) sql.NullString {
	return sql.NullString{String : s, Valid : s != ""}
}

func ToIntegerVal(i int64) string {
	var valueInt string
	valueInt = strconv.FormatInt(int64(i), 10)
	return valueInt
}

func ToStringFromInt(i int) string {
	var valueInt string
	valueInt = strconv.Itoa(i)
	return valueInt
}

func ToString(s sql.NullString) string {
	var valInString string
	if(s.Valid) {
		valInString = s.String
		fmt.Println(valInString)
	} else {
		valInString = "NULL"
		fmt.Println(valInString)
	}
	return valInString
}

type userDetails struct {
	AppVersion         sql.NullString `json:"app_version"`
	AttributeBits      int    `json:"attributeBits"`
	Circle             int    `json:"circle"`
	DevID              sql.NullString `json:"dev_id"`
	DevToken           sql.NullString `json:"dev_token"`
	DevTokenUpdateTs   int64    `json:"dev_token_update_ts"`
	DevType            sql.NullString `json:"dev_type"`
	DevVersion         sql.NullString `json:"dev_version"`
	DeviceKey          sql.NullString `json:"device_key"`
	EndTime            int64    `json:"end_time"`
	LastActivityTime   int64    `json:"last_activity_time"`
	Msisdn             int64    `json:"msisdn"`
	Operator           string `json:"operator"`
	OriginalAppVersion sql.NullString `json:"original_app_version"`
	Os                 sql.NullString `json:"os"`
	OsVersion          sql.NullString `json:"os_version"`
	Pdm                string `json:"pdm"`
	RegTime            int64    `json:"reg_time"`
	Resolution         string `json:"resolution"`
	Sound              sql.NullString `json:"sound"`
	Token              sql.NullString `json:"token"`
	UID                sql.NullString `json:"uid"`
	UpgradeTime        int64    `json:"upgrade_time"`
}
