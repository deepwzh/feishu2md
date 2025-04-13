package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/88250/lute"
	"github.com/Wsine/feishu2md/core"
	"github.com/Wsine/feishu2md/utils"
	"github.com/chyroc/lark"
	"github.com/pkg/errors"
)

type DownloadOpts struct {
	outputDir string
	dump      bool
	batch     bool
	wiki      bool

	imageBaseDir string
}

var dlOpts = DownloadOpts{}
var dlConfig core.Config

type donwloadDocumentResp struct {
	Path  string `json:"path"`
	Title string `json:"title"`
}

func downloadDocument(ctx context.Context, client *core.Client, url string, opts *DownloadOpts) (*donwloadDocumentResp, error) {
	// Validate the url to download
	docType, docToken, err := utils.ValidateDocumentURL(url)
	if err != nil {
		return nil, err
	}
	fmt.Println("Captured document token:", docToken)

	// for a wiki page, we need to renew docType and docToken first
	if docType == "wiki" {
		node, err := client.GetWikiNodeInfo(ctx, docToken)
		if err != nil {
			err = fmt.Errorf("GetWikiNodeInfo err: %v for %v", err, url)
		}
		utils.CheckErr(err)
		docType = node.ObjType
		docToken = node.ObjToken
	}
	if docType == "docs" {
		return nil, errors.Errorf(
			`Feishu Docs is no longer supported. ` +
				`Please refer to the Readme/Release for v1_support.`)
	}

	// Process the download
	docx, blocks, err := client.GetDocxContent(ctx, docToken)
	utils.CheckErr(err)

	parser := core.NewParser(dlConfig.Output)

	title := docx.Title
	markdown := parser.ParseDocxContent(docx, blocks)

	if !dlConfig.Output.SkipImgDownload {
		for _, imgToken := range parser.ImgTokens {
			localLink, err := client.DownloadImage(
				ctx, imgToken, filepath.Join(opts.outputDir, dlConfig.Output.ImageDir),
			)
			if err != nil {
				return nil, err
			}
			markdown = strings.Replace(markdown, imgToken, opts.imageBaseDir+localLink, 1)
		}
		for _, boardToken := range parser.BoardTokens {
			localLink, err := client.DownloadBoardImage(
				ctx, boardToken, filepath.Join(opts.outputDir, dlConfig.Output.ImageDir),
			)
			if err != nil {
				return nil, err
			}
			markdown = strings.Replace(markdown, boardToken, opts.imageBaseDir+localLink, 1)
		}
	}

	// Format the markdown document
	engine := lute.New(func(l *lute.Lute) {
		l.RenderOptions.AutoSpace = true
	})
	result := engine.FormatStr("md", markdown)

	// Handle the output directory and name
	if _, err := os.Stat(opts.outputDir); os.IsNotExist(err) {
		if err := os.MkdirAll(opts.outputDir, 0o755); err != nil {
			return nil, err
		}
	}

	if dlOpts.dump {
		jsonName := fmt.Sprintf("%s.json", docToken)
		outputPath := filepath.Join(opts.outputDir, jsonName)
		data := struct {
			Document *lark.DocxDocument `json:"document"`
			Blocks   []*lark.DocxBlock  `json:"blocks"`
		}{
			Document: docx,
			Blocks:   blocks,
		}
		pdata := utils.PrettyPrint(data)

		if err = os.WriteFile(outputPath, []byte(pdata), 0o644); err != nil {
			return nil, err
		}
		fmt.Printf("Dumped json response to %s\n", outputPath)
	}

	// Write to markdown file
	mdName := fmt.Sprintf("%s.md", docToken)
	if dlConfig.Output.TitleAsFilename {
		mdName = fmt.Sprintf("%s.md", utils.SanitizeFileName(title))
	}
	outputPath := filepath.Join(opts.outputDir, mdName)
	if err = os.WriteFile(outputPath, []byte(result), 0o644); err != nil {
		return nil, err
	}
	fmt.Printf("Downloaded markdown file to %s\n", outputPath)

	return &donwloadDocumentResp{
		Path:  outputPath,
		Title: title,
	}, nil
}

func downloadDocuments(ctx context.Context, client *core.Client, url string) error {
	// Validate the url to download
	folderToken, err := utils.ValidateFolderURL(url)
	if err != nil {
		return err
	}
	fmt.Println("Captured folder token:", folderToken)

	// Error channel and wait group
	errChan := make(chan error)
	wg := sync.WaitGroup{}

	// Recursively go through the folder and download the documents
	var processFolder func(ctx context.Context, folderPath, folderToken string) error
	processFolder = func(ctx context.Context, folderPath, folderToken string) error {
		files, err := client.GetDriveFolderFileList(ctx, nil, &folderToken)
		if err != nil {
			return err
		}
		opts := DownloadOpts{outputDir: folderPath, dump: dlOpts.dump, batch: false}
		for _, file := range files {
			if file.Type == "folder" {
				_folderPath := filepath.Join(folderPath, file.Name)
				if err := processFolder(ctx, _folderPath, file.Token); err != nil {
					return err
				}
			} else if file.Type == "docx" {
				// concurrently download the document
				wg.Add(1)
				go func(_url string) {
					if _, err := downloadDocument(ctx, client, _url, &opts); err != nil {
						errChan <- err
					}
					wg.Done()
				}(file.URL)
			}
		}
		return nil
	}
	if err := processFolder(ctx, dlOpts.outputDir, folderToken); err != nil {
		return err
	}

	// Wait for all the downloads to finish
	go func() {
		wg.Wait()
		close(errChan)
	}()
	for err := range errChan {
		return err
	}
	return nil
}

func readLastNodeCache(spaceID string) (map[string]*WikeNodeItem, error) {
	path := filepath.Join(".cache", spaceID+".json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return make(map[string]*WikeNodeItem), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]*WikeNodeItem)
	if err = json.Unmarshal(data, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func saveLastNodeCache(spaceID string, nodes map[string]*WikeNodeItem) error {
	path := filepath.Join(".cache", spaceID+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(nodes)
	if err != nil {
		return err
	}
	if err = os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	return nil
}

type WikeNodeItem struct {
	NodeToken   string `json:"node_token"`
	ObjEditTime string `json:"obj_edit_time"`
	ObjType     string `json:"obj_type"`
	Title       string `json:"title"`
	Path        string `json:"path"`
}

func downloadWiki(ctx context.Context, client *core.Client, url string) error {
	prefixURL, spaceID, err := utils.ValidateWikiURL(url)
	if err != nil {
		return err
	}

	folderPath, err := client.GetWikiName(ctx, spaceID)
	if err != nil {
		return err
	}
	if folderPath == "" {
		return fmt.Errorf("failed to GetWikiName")
	}

	oldWikiNodes, _ := readLastNodeCache(spaceID)
	// 记录当前所有文档的树状结构，用来做增量的下载支持
	wikiNodes := make(map[string]*WikeNodeItem)

	errChan := make(chan error)

	var maxConcurrency = 10 // Set the maximum concurrency level
	wg := sync.WaitGroup{}
	semaphore := make(chan struct{}, maxConcurrency) // Create a semaphore with the maximum concurrency level

	var downloadWikiNode func(ctx context.Context,
		client *core.Client,
		spaceID string,
		parentPath string,
		parentNodeToken *string) error

	downloadWikiNode = func(ctx context.Context,
		client *core.Client,
		spaceID string,
		folderPath string,
		parentNodeToken *string) error {
		nodes, err := client.GetWikiNodeList(ctx, spaceID, parentNodeToken)
		if err != nil {
			return err
		}
		for _, n := range nodes {
			wikiNodeItem := &WikeNodeItem{
				NodeToken:   n.NodeToken,
				ObjType:     n.ObjType,
				ObjEditTime: n.ObjEditTime,
				Title:       n.Title,
			}
			// if n.ObjEditTime
			if n.HasChild {
				_folderPath := filepath.Join(folderPath, n.Title)
				if err := downloadWikiNode(ctx, client,
					spaceID, _folderPath, &n.NodeToken); err != nil {
					return err
				}
			}
			if n.ObjType == "docx" {
				//
				if oldNode, ok := oldWikiNodes[n.NodeToken]; ok {
					// 如果新节点的修改时间晚于旧节点的修改时间，需要重新下载；否则跳过下载
					if oldNode.ObjEditTime >= n.ObjEditTime {
						wikiNodeItem.Title = oldNode.Title
						wikiNodeItem.Path = oldNode.Path
						wikiNodes[n.NodeToken] = wikiNodeItem
						continue
					}
				}
				opts := DownloadOpts{outputDir: folderPath, dump: dlOpts.dump, batch: false, imageBaseDir: dlOpts.imageBaseDir}
				wg.Add(1)
				semaphore <- struct{}{}
				go func(_url string) {
					resp, err := downloadDocument(ctx, client, _url, &opts)
					if err != nil {
						errChan <- err
					} else {
						wikiNodeItem.Title = resp.Title
						wikiNodeItem.Path = resp.Path
					}
					wg.Done()
					<-semaphore
				}(prefixURL + "/wiki/" + n.NodeToken)
				// downloadDocument(ctx, client, prefixURL+"/wiki/"+n.NodeToken, &opts)
			}
			wikiNodes[n.NodeToken] = wikiNodeItem
		}
		return nil
	}

	if err = downloadWikiNode(ctx, client, spaceID, folderPath, nil); err != nil {
		return err
	}

	// 新增：计算需要删除的文件列表
	var filesToDelete []string
	for token, oldNode := range oldWikiNodes {
		if _, exists := wikiNodes[token]; !exists {
			filesToDelete = append(filesToDelete, oldNode.Path)
		}
	}

	for _, filePath := range filesToDelete {
		if err := os.Remove(filePath); err == nil {
			fmt.Printf("Deleted file: %s\n", filePath)
		}
	}

	saveLastNodeCache(spaceID, wikiNodes)

	// Wait for all the downloads to finish
	go func() {
		wg.Wait()
		close(errChan)
	}()
	for err := range errChan {
		return err
	}
	return nil
}

func handleDownloadCommand(url string) error {
	// Load config
	configPath, err := core.GetConfigFilePath()
	if err != nil {
		return err
	}
	config, err := core.ReadConfigFromFile(configPath)
	if err != nil {
		return err
	}
	dlConfig = *config

	// Instantiate the client
	client := core.NewClient(
		dlConfig.Feishu.AppId, dlConfig.Feishu.AppSecret,
	)
	ctx := context.Background()

	dlOpts.imageBaseDir = dlConfig.Output.ImageBaseDir

	if dlOpts.batch {
		return downloadDocuments(ctx, client, url)
	}

	if dlOpts.wiki {
		return downloadWiki(ctx, client, url)
	}

	_, err = downloadDocument(ctx, client, url, &dlOpts)
	return err
}
