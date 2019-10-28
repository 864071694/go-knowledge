package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		fmt.Println(path, "已经存在")
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func downloadFile(url string, localPath string, fb func(length, downLen int64)) error {
	var (
		fSize     int64
		buf       = make([]byte, 32*1024)
		totalSize int64
	)
	tmpFilePath := localPath + ".tmp"
	fmt.Println("临时文件路径为", tmpFilePath)
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Header.Add("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1")
	var bytesSize int64 = 0
	exists, err := PathExists(localPath)
	if exists {
		fmt.Println("文件已下载")
		return nil
	}
	if err != nil {
		fmt.Println("文件不存在")
		return err
	}
	exists, err = PathExists(tmpFilePath)
	if exists {
		fileInfo, err := os.Stat(tmpFilePath)
		if err != nil {
			fmt.Println("获取文件信息失败")
			return err
		}
		bytesSize = fileInfo.Size()
		fmt.Println("准备断点下载,当前大小为", fileInfo.Size())
	} else {
		fmt.Println("尚未下载文件")
		bytesSize = 0
	}
	request.Header.Add("Range", "bytes="+strconv.FormatInt(bytesSize, 10)+"-")
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	//读取服务器返回的文件大小
	fSize, err = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 32)
	if err != nil {
		return err
	}
	exists, err = PathExists(localPath)
	if exists {
		return err
	}
	totalSize = bytesSize + fSize
	fmt.Println("文件总大小为：", totalSize)
	//打开文件
	file, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_APPEND, 0666)
	defer file.Close()
	if err != nil {
		fmt.Println("文件打开错误")
		file, err = os.Create(tmpFilePath)
	}
	stat, err := file.Stat() //获取文件状态
	if err != nil {
		return err
	}
	file.Seek(stat.Size(), 0) //把文件指针指到文件末，当然你说为何不直接用 O_APPEND 模式打开，没错是可以。我这里只是试验。
	defer file.Close()
	if resp.Body == nil {
		return errors.New("body is null")
	}
	defer resp.Body.Close()
	for {
		//读取bytes
		nr, er := resp.Body.Read(buf)
		if nr > 0 {
			//写入bytes
			nw, ew := file.Write(buf[0:nr])
			//数据长度大于0
			if nw > 0 {
				bytesSize += int64(nw)
			}
			//写入出错
			if ew != nil {
				err = ew
				break
			}
			//读取是数据长度不等于写入的数据长度
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		fb(totalSize, bytesSize)
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
		if bytesSize > totalSize {
			break
		}
	}
	if err == nil {
		file.Close()
		err = os.Rename(tmpFilePath, localPath)
	}
	return err
}
func progress(length, down_length int64) {
	jindu := float64(down_length) / float64(length) * 100
	fmt.Println("已经下载了", fmt.Sprintf("%.2f", jindu))
	if jindu >= 100 {
		fmt.Printf("下载完成")
	}
}
func main() {
	downloadFile("http://ali.static.yximgs.com/udata/pkg/fe/kwai_video.c8e339ab.mp4", "D:\\a.mp4", progress)
}
