package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shopspring/decimal"
	"golang.org/x/net/context"
)

type Wallet struct {
	Id      string
	Balance decimal.Decimal
}

var walletsTableCreateSql = `
	create table if not exists wallets (
		id text not null primary key, 
		balance decimal not null
		);
`

type WalletTransaction struct {
	AuthorId string
	SenderId string
	Balance  decimal.Decimal
	Date     sql.NullTime
}

type WalletTransactionDTO struct {
	AuthorId string          `json:"from"`
	SenderId string          `json:"to"`
	Balance  decimal.Decimal `json:"amount"`
	Date     string          `json:"time"`
}

var walletsHistoryTableCreateSql = `
	create table if not exists wallet_transactions (
		author_id text not null, 
		sender_id text not null, 
		balance decimal not null,
		date timestamp not null,

		foreign key (author_id) references wallets (id),
		foreign key (sender_id) references wallets (id)
		);
`

func init() {
	assertAvailablePRNG()
}

func assertAvailablePRNG() {
	// Assert that a cryptographically secure PRNG is available.
	// Panic otherwise.
	buf := make([]byte, 1)

	_, err := io.ReadFull(rand.Reader, buf)
	if err != nil {
		panic(fmt.Sprintf("crypto/rand is unavailable: Read() failed with %#v", err))
	}
}

// GenerateRandomBytes returns securely generated random bytes.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

// GenerateRandomString returns a securely generated random string.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomString(n int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		ret[i] = letters[num.Int64()]
	}

	return string(ret), nil
}

// GenerateRandomStringURLSafe returns a URL-safe, base64 encoded
// securely generated random string.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomStringURLSafe(n int) (string, error) {
	b, err := GenerateRandomBytes(n)
	return base64.URLEncoding.EncodeToString(b), err
}

type Result struct {
	Wallet Wallet
	Err    error
}

func loadByIdAsync(db *sql.Tx, channel chan Result, id string) {
	var wallet Wallet
	err := db.QueryRow("select * from wallets where id = ?", id).Scan(&wallet.Id, &wallet.Balance)
	channel <- Result{
		Wallet: wallet,
		Err:    err,
	}
}

type SendWalletRequestBody struct {
	ID     string          `json:"to"`
	Amount decimal.Decimal `json:"amount"`
}

func main() {
	db, err := sql.Open("sqlite3", "./data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sqlStmt := walletsTableCreateSql + walletsHistoryTableCreateSql

	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}

	r := gin.Default()
	v1 := r.Group("/api/v1/wallet")
	{
		//curl -d "" http://localhost:8080/api/v1/wallet/
		v1.POST("", func(c *gin.Context) {

			// for my (and your) own convinience, i'll generate a random id of length 6
			// it will be fine for this example app, but for a real application there should be either a retry policy, or a much bigger id
			id, err := GenerateRandomString(6)
			if err != nil {
				panic(err)
			}

			_, err = db.Exec("insert into wallets(id, balance) values(?,100)", id)

			if err != nil {
				log.Fatal(err)
				c.JSON(http.StatusInternalServerError, gin.H{
					"id": id,
				})
			} else {
				c.JSON(http.StatusCreated, gin.H{
					"id":      id,
					"balance": 100,
				})
			}

		})

		//curl --json '{"to":"TTTFGF","amount":10}' http://localhost:8080/api/v1/wallet/TTTFGF/send
		v1.POST(":walletid/send", func(c *gin.Context) {
			// this is a weird endpoint, because there is a lot of undefined behaviour.

			// what happens when fromId == toId?
			// Idk, so i'll allow those empty transactions to happen.
			// So, you can transfer money to yourself, but it'll fail if you don't have enough money to do it.
			// What a fancy way to check if your balance is above a certain threshold...

			// what happens when amount is negative or zero?
			// Idk, so i'll allow that as well.
			// So, you can basically steal money from other people's wallets by specifying negative amount;
			// It can also make other people's wallets negative, but that's a very weird thing to do.
			// so i'll validate BOTH receiver and sender balances, even though it is not stated in the problem.

			var requestBody SendWalletRequestBody
			if err := c.ShouldBindJSON(&requestBody); err != nil {
				log.Println(err)
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			fromId := c.Param("walletid")
			toId := requestBody.ID
			amount := requestBody.Amount

			ctx := context.Background()

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				tx.Rollback()
				log.Fatal(err)
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}

			fromCh := make(chan Result)
			toCh := make(chan Result)

			go loadByIdAsync(tx, fromCh, fromId)
			go loadByIdAsync(tx, toCh, toId)

			log.Println("walletResFrom")
			walletResFrom := <-fromCh
			if walletResFrom.Err != nil {
				tx.Rollback()
				log.Println(walletResFrom.Err)
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			log.Println("walletResTo")
			log.Println(toId)
			walletResTo := <-toCh
			if walletResTo.Err != nil {
				tx.Rollback()
				log.Println(walletResTo.Err)
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}

			fromAmount := walletResFrom.Wallet.Balance.Sub(amount)
			log.Println(fromAmount)
			toAmount := walletResTo.Wallet.Balance.Add(amount)
			log.Println(toAmount)

			if !fromAmount.IsPositive() || !toAmount.IsPositive() {
				log.Println(err)
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			_, err = tx.ExecContext(ctx, `
					update wallets set balance = ? where id = ? ;
					update wallets set balance = ? where id = ? ;
					insert into wallet_transactions(author_id, sender_id, balance, date) values(?,?,?,?);
				`, fromAmount, fromId, toAmount, toId, fromId, toId, amount, time.Now())

			if err != nil {
				log.Println(err)
				tx.Rollback()
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			err = tx.Commit()
			if err != nil {
				log.Println(err)
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			c.Status(http.StatusOK)
		})

		//curl http://localhost:8080/api/v1/wallet/TTTFGF/history
		v1.GET(":walletid/history", func(c *gin.Context) {
			userInputId := c.Param("walletid")
			var wallet Wallet
			err := db.QueryRow(`select * from wallets where id = ? limit 1`, userInputId).Scan(&wallet.Id, &wallet.Balance)
			if err != nil {
				log.Println(err)
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			response, err := db.Query(`select * from wallet_transactions 
			where author_id = ? or sender_id = ?`, userInputId, userInputId)
			if err != nil {
				log.Println(err)
				c.AbortWithStatus(http.StatusNotFound)
				return
			}

			defer response.Close()

			var rows []WalletTransactionDTO = []WalletTransactionDTO{}

			for response.Next() {
				var row WalletTransaction
				err = response.Scan(&row.AuthorId, &row.SenderId, &row.Balance, &row.Date)
				if err != nil {
					log.Println(err)
					c.AbortWithStatus(http.StatusNotFound)
					return
				}
				rows = append(rows, WalletTransactionDTO{
					AuthorId: row.AuthorId,
					SenderId: row.SenderId,
					Balance:  row.Balance,
					Date:     row.Date.Time.Format(time.RFC3339),
				})
			}

			c.JSON(http.StatusOK, rows)
		})
 
		//curl http://localhost:8080/api/v1/wallet/TTTFGF
		v1.GET(":walletid", func(c *gin.Context) {
			userInputId := c.Param("walletid")
			var wallet Wallet
			err := db.QueryRow(`select * from wallets 
			where id = ? limit 1`, userInputId).Scan(&wallet.Id, &wallet.Balance)
			if err != nil {
				c.AbortWithStatus(http.StatusNotFound)
			} else {
				c.JSON(http.StatusOK, gin.H{
					"id":      wallet.Id,
					"balance": wallet.Balance,
				})
			}
		})
	}
	r.Run(":8080")
}
