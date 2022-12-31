package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/log"
	"go.uber.org/zap"
)

var (
	restoreBackupNameV2      string
	restoreCollectionNamesV2 string
	renameSuffixV2           string
	renameCollectionNamesV2  string

	coll      string
	partition string
	//files     string
	endTime int64
)

var restoreBackupCmdV2 = &cobra.Command{
	Use:   "restore_v2",
	Short: "restore subcommand restore a backup.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		fmt.Println("config:" + config)
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		//var collectionNameArr []string
		//if collectionNames == "" {
		// collectionNameArr = []string{}
		//} else {
		// collectionNameArr = strings.Split(restoreCollectionNames, ",")
		//}

		//var renameMap map[string]string
		//if renameCollectionNames == "" {
		// renameMap = map[string]string{}
		//} else {
		// renameArr := strings.Split(renameCollectionNames, ",")
		// if len(renameArr) != len(collectionNameArr) {
		//  fmt.Errorf("collection_names and renames num dismatch, Forbid to restore")
		// }
		//}

		//resp := backupContext.RestoreBackup(context, &backuppb.RestoreBackupRequest{
		// BackupName:        restoreBackupName,
		// CollectionNames:   collectionNameArr,
		// CollectionSuffix:  renameSuffix,
		// CollectionRenames: renameMap,
		//})
		var fileArray []string
		//if files == "" {
		//	fileArray = []string{}
		//} else {
		//	fileArray = strings.Split(files, ",")
		//}
		insertPath := fmt.Sprintf("%s/%s/%s/%v/%v/", "backup/go_backup_test", "binlogs", "insert_log", 437840066971907131, 437840066971907132)

		fileArray = []string{insertPath, ""}

		err := backupContext.ExecuteBulkInsert(context, coll, partition, fileArray, endTime)
		if err != nil {
			log.Error("fail to bulk insert",
				zap.Error(err),
				zap.String("collectionName", coll),
				zap.String("partitionName", partition),
				zap.Strings("files", fileArray))
		}
		//fmt.Println(resp.GetCode(), "\n", resp.GetMsg())
	},
}

func init() {
	restoreBackupCmdV2.Flags().StringVarP(&restoreBackupNameV2, "name2", "n", "", "backup name to restore")
	restoreBackupCmdV2.Flags().StringVarP(&restoreCollectionNamesV2, "collections2", "c", "", "collectionNames to restore")
	restoreBackupCmdV2.Flags().StringVarP(&renameSuffixV2, "suffix2", "s", "", "add a suffix to collection name to restore")
	restoreBackupCmdV2.Flags().StringVarP(&renameCollectionNamesV2, "rename2", "r", "", "rename collections to new names")

	restoreBackupCmdV2.Flags().StringVarP(&coll, "coll", "o", "", "coll string")
	restoreBackupCmdV2.Flags().StringVarP(&partition, "partition", "a", "", "partition string")
	//restoreBackupCmdV2.Flags().StringVarP(&files, "files", "i", "", "files string array")
	restoreBackupCmdV2.Flags().Int64VarP(&endTime, "endTime", "t", 0, "endTime")

	rootCmd.AddCommand(restoreBackupCmdV2)
}
