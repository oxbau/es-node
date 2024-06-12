package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

const (
	batchSize    = 100
	twitterToken = "AAAAAAAAAAAAAAAAAAAAANi0uAEAAAAAYprlmmVAGPG1NCZx6aDVsi0XtxE%3DTB4xjPZMngFzp6QGkI5SAT3HwyH61zPwahcX94cmF8dGXQeQnq"
)

var (
	inputFlag     = flag.String("input", "input.csv", "input file for filtering")
	passFlag      = flag.String("pass", "pass.csv", "records pass filter rule")
	filteroutFlag = flag.String("filterout", "filterout.csv", "records do not pass filter rule")
	toRerunFlag   = flag.String("torerun", "torerun.csv", "records fail to request, to be rerun later")
)

type Result struct {
	Data []struct {
		AuthorID string `json:"author_id"`
		Text     string `json:"text"`
		ID       string `json:"id"`
	} `json:"data"`
	Includes struct {
		Users []struct {
			ID          string    `json:"id"`
			Username    string    `json:"username"`
			Name        string    `json:"name"`
			CreatedAt   time.Time `json:"created_at"`
			Description string    `json:"description"`
			Verified    bool      `json:"verified"`
			Metrics     struct {
				Followers  int `json:"followers_count"`
				TweetCount int `json:"tweet_count"`
			} `json:"public_metrics"`
		} `json:"users"`
	} `json:"includes"`
	Errors []struct {
		Tweet  string `json:"value"`
		Detail string `json:"detail"`
	} `json:"errors"`
}

type Tweet struct {
	TweetId  string
	TweetUrl string
	UserId   string
	Text     string
}

type TwitterUser struct {
	Id          string
	Account     string
	Name        string
	Description string
	CreateAt    time.Time

	Followers  int
	TweetCount int
}

type Record struct {
	RecordId   int
	RecodeTime string
	Address    common.Address
	Email      string

	TwitterUser *TwitterUser
	Tweet       *Tweet

	Error string

	line string
}

func (r *Record) String() string {
	return fmt.Sprintf("\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%v\",\"%s\",\"%s\",\"%s\"\n",
		r.RecordId, r.RecodeTime, r.Address.Hex(), r.Email, r.TwitterUser.CreateAt.Format("2006-01-02"), r.Tweet.TweetUrl, r.TwitterUser.Account, r.TwitterUser.Followers, r.TwitterUser.TweetCount,
		strings.Contains(r.Tweet.Text, "@EthStorage"), removeCharacters(r.TwitterUser.Name), removeCharacters(r.TwitterUser.Description), removeCharacters(r.Tweet.Text))
}

func (r *Record) StringError() string {
	return fmt.Sprintf("\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
		r.RecordId, r.RecodeTime, r.Address.Hex(), r.Email, r.Tweet.TweetUrl, r.Error)
}

func removeCharacters(in string) string {
	in = strings.Replace(in, "“", "", 100)
	in = strings.Replace(in, "”", "", 100)
	in = strings.Replace(in, "\"", "", 100)
	in = strings.Replace(in, "\r", "", 100)
	return strings.Replace(in, "\n", "", 100)
}

func PassHeader() string {
	return "\"RecodeId\",\"RecodeTime\",\"Address\",\"Email\",\"CreateAt\",\"TweetUrl\",\"Account\",\"Followers\",\"TweetCount\",\"@EthStorage\",\"UserName\",\"Description\",\"Text\"\n"
}

func FilterOutHeader() string {
	return "\"RecodeId\",\"RecodeTime\",\"Address\",\"Email\",\"TweetUrl\",\"Error\"\n"
}

type Filter struct {
	inputFile     *os.File
	passFile      *os.File
	filterOutFile *os.File
	toRerunFile   *os.File
}

func main() {
	// Parse the flags and set up the logger to print everything requested
	flag.Parse()

	input, err := os.Open(*inputFlag)
	if err != nil {
		log.Crit(err.Error())
	}
	defer input.Close()

	pass, err := os.OpenFile(*passFlag, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Crit(err.Error())
	}
	defer pass.Close()

	filterOut, err := os.OpenFile(*filteroutFlag, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Crit(err.Error())
	}
	defer filterOut.Close()

	toRerun, err := os.OpenFile(*toRerunFlag, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Crit(err.Error())
	}
	defer toRerun.Close()

	filter := Filter{
		inputFile:     input,
		passFile:      pass,
		filterOutFile: filterOut,
		toRerunFile:   toRerun,
	}

	filter.StartFiltering()
}

func (f *Filter) StartFiltering() {
	scanner := bufio.NewScanner(f.inputFile)

	// write header -- first line
	f.passFile.WriteString(PassHeader())
	f.filterOutFile.WriteString(FilterOutHeader())

	recordId := 0
	batch := make([]*Record, 0)
	for scanner.Scan() {
		// "2024/03/14 7:05:40 PM GMT+8","","0xd8367A027FAB084a8654F2C3132288Ef372539A6","https://twitter.com/liheact/status/1768231011244970157","","liheact@gmail.com"
		// "2024/03/15 6:04:37 PM GMT+8","ucoxrey@gmail.com","0x5260c7717325B4B872d45a461cA669B4Cda64fD8","https://x.com/0xreynn/status/1768578978229665885?s=20",""
		// 1314,"0x3C8B97f934B37D03EE49fe27f17f054466eCDdD7","https://twitter.com/wordorexia/status/1769220169648468340?t=nL58F-72j1CAoEkGcqkEvg&s=19","wordorexia@gmail.com","Wordorexia Writer","3/17/2024, 12:34:18 PM","f6b3cec08952f1"
		line := scanner.Text()
		l := strings.Replace(line, "\"", "", 100)
		items := strings.Split(l, ",")
		if len(items) < 4 {
			continue
		}
		//	recordTime, address, email, twitterUrl := items[0], items[2], items[5], items[3]
		// recordTime, address, email, twitterUrl := items[0], items[2], items[1], items[3]
		recordTime, address, email, twitterUrl := items[5], items[1], items[3], items[2]

		if !strings.HasPrefix(items[2], "https://twitter.com/") && !strings.HasPrefix(twitterUrl, "https://x.com/") {
			continue
		}

		tweetId, err := getTweetID(twitterUrl)
		if err != nil {
			if tweetId != "" {
				f.filterOutFile.WriteString(fmt.Sprintf("\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
					0, recordTime, address, email, twitterUrl, err.Error()))
			}
			continue
		}

		batch = append(batch, &Record{
			RecordId:   recordId,
			RecodeTime: recordTime,
			Address:    common.HexToAddress(address),
			Email:      email,
			Tweet: &Tweet{
				TweetId:  tweetId,
				TweetUrl: twitterUrl,
			},
			line: line,
		})
		recordId++

		if len(batch)%batchSize == 0 {
			fmt.Println(recordId)
			f.FetchBatchAndOutput(batch)
			batch = make([]*Record, 0)
			time.Sleep(time.Minute)
		}
	}

	if len(batch) > 0 {
		f.FetchBatchAndOutput(batch)
		batch = make([]*Record, 0)
	}
}

func (f *Filter) Reformat(batch []*Record) {
	for _, r := range batch {
		f.toRerunFile.WriteString(fmt.Sprintf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
			r.RecodeTime, r.Email, r.Address.Hex(), r.Tweet.TweetUrl, ""))
	}
}

func (f *Filter) FetchBatchAndOutput(batch []*Record) {
	tweetIDs := ""
	for _, r := range batch {
		if tweetIDs == "" {
			tweetIDs = r.Tweet.TweetId
		} else {
			tweetIDs = tweetIDs + "," + r.Tweet.TweetId
		}
	}

	result, err := authTwitterWithToken(tweetIDs, twitterToken)
	if err != nil {
		fmt.Println("fetch error", err.Error())
		for _, r := range batch {
			f.toRerunFile.WriteString(fmt.Sprintf("%s\n", r.line))
		}
		return
	}

	userMap := make(map[string]*TwitterUser)
	tweetMap := make(map[string]*Tweet)
	errorMap := make(map[string]string)

	for _, user := range result.Includes.Users {
		userMap[user.ID] = &TwitterUser{
			Id:          user.ID,
			Account:     user.Username,
			Name:        user.Name,
			Description: user.Description,
			CreateAt:    user.CreatedAt,
			Followers:   user.Metrics.Followers,
			TweetCount:  user.Metrics.TweetCount,
		}
	}

	for _, tweet := range result.Data {
		tweetMap[tweet.ID] = &Tweet{
			TweetId: tweet.ID,
			UserId:  tweet.AuthorID,
			Text:    tweet.Text,
		}
	}

	for _, e := range result.Errors {
		errorMap[e.Tweet] = e.Detail
	}

	for _, r := range batch {
		if err, ok := errorMap[r.Tweet.TweetId]; ok {
			r.Error = err
			f.filterOutFile.WriteString(r.StringError())
			continue
		}
		tweet, ok := tweetMap[r.Tweet.TweetId]
		if !ok {
			f.toRerunFile.WriteString(r.line)
			continue
		}
		user, ok := userMap[tweet.UserId]
		if !ok {
			r.Error = fmt.Sprintf("Twitter user %s not found in the response", tweet.UserId)
			f.filterOutFile.WriteString(r.StringError())
			continue
		}

		address := common.HexToAddress(regexp.MustCompile("0x[0-9a-fA-F]{40}").FindString(tweet.Text))
		if r.Address.Cmp(address) != 0 {
			r.Error = fmt.Sprintf("Tweet text do not contain the same address filled in the form, address in form %s, address in tweet %s",
				r.Address, address)
			f.filterOutFile.WriteString(r.StringError())
			continue
		}

		if user.Followers < 100 {
			r.Error = fmt.Sprintf("Twitter user has less than 100 followers (%d).", user.Followers)
			f.filterOutFile.WriteString(r.StringError())
			continue
		}

		if user.TweetCount < 10 {
			r.Error = fmt.Sprintf("Twitter user has less than 10 tweets (%d).", user.TweetCount)
			f.filterOutFile.WriteString(r.StringError())
			continue
		}

		if user.CreateAt.Compare(time.Now().AddDate(0, -6, 0)) > 0 {
			r.Error = fmt.Sprintf("Twitter user created time less than 6 months (%v).", user.CreateAt)
			f.filterOutFile.WriteString(r.StringError())
			continue
		}

		r.Tweet.UserId = tweet.UserId
		r.Tweet.Text = tweet.Text
		r.TwitterUser = user
		f.passFile.WriteString(r.String())
	}
}

func getTweetID(url string) (string, error) {
	// Ensure the user specified a meaningful URL, no fancy nonsense
	parts := strings.Split(url, "/")
	if len(parts) < 4 || parts[len(parts)-2] != "status" {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", errors.New("Invalid Twitter status URL")
	}
	// Strip any query parameters from the Tweet id and ensure it's numeric
	tweetID := strings.Split(parts[len(parts)-1], "?")[0]
	if !regexp.MustCompile("^[0-9]+$").MatchString(tweetID) {
		return "", errors.New("Invalid Tweet URL")
	}
	if !strings.HasPrefix(tweetID, "1") {
		return tweetID, errors.New(fmt.Sprintf("invalid tweet ID %s, Id should start with 1", tweetID))
	}
	return tweetID, nil
}

func authTwitterWithToken(tweetIDs string, token string) (*Result, error) {
	// Query the Tweet details from Twitter
	url := fmt.Sprintf("https://api.twitter.com/2/tweets/?ids=%s&expansions=author_id&user.fields=created_at,public_metrics,verified,description", tweetIDs)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result Result

	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
