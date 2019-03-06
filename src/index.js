const fs = require('fs')
const xlsx = require('node-xlsx')

const EXPORT_DATA_CATEGORY_ID = '5c711e3119111409cdabe6f2'    // 文件上传分类 id
const TABLE_ID = {
  order: 66666,         // 订单表
  export_task: 66667,   // 导出任务记录表
}

const TMP_FILE_NAME = '/tmp/result.xlsx'  // 本地临时文件路径，以 /tmp 开头，具体请查看：https://doc.minapp.com/support/technical-notes.html (云函数的临时文件存储)
const ROW_NAME = ['name', 'price']        // Excel 文件列名配置
const MAX_CONNECT_LIMIT = 5               // 最大同时请求数
const LIMIT = 1000                        // 单次最大拉取数据数
let result = []

/**
 * 更新导出记录中的 file_download_link 字段
 * @param {*} tableID 
 * @param {*} recordId 
 * @param {*} fileLink 
 */
function updateExportJobIdRecord(tableID, recordId, fileLink) {
  let Schame = new BaaS.TableObject(tableID)
  let schame = Schame.getWithoutData(recordId)

  schame.set('file_download_link', fileLink)
  return schame.update()
}

/**
 * 创建数据导出任务
 * 设置初始 file_download_link 为空
 * 待导出任务执行完毕后将文件下载地址存储到 file_download_link 字段中
 * @param {*} tableID 
 */
function createExportJobIdRecord(tableID) {
  let Schame = new BaaS.TableObject(tableID)
  let schame = Schame.create()
  return schame.set({file_download_link: ''}).save().then(res => {
    return res.data.id
  })
}

/**
 * 获取总数据条数
 * @tableId {*} tableId
 */
function getTotalCount(tableId) {
  const Order = new BaaS.TableObject(tableId)
  return Order.count()
    .then(num => {
      console.log('数据总条数:', num)
      return num
    })
    .catch(err => {
      console.log('获取数据总条数失败:', err)
      throw new Error(err)
    })
}

/**
 * 分批拉取数据
 * @param {*} tableId 
 * @param {*} offset 
 * @param {*} limit 
 */
function getDataByGroup(tableId, offset = 0, limit = LIMIT) {
  let Order = new BaaS.TableObject(tableId)
  return Order.limit(limit).offset(offset).find()
    .then(res => {
      return res.data.objects
    })
    .catch(err => {
      console.log('获取分组数据失败:', err)
      throw new Error(err)
    })
}

/**
 * 创建 Excel 导出文件
 * @param {*} sourceData 源数据
 */
function genExportFile(sourceData = []) {
  const resultArr = []
  const rowArr = []

  // 配置列名
  rowArr.push(ROW_NAME)

  sourceData.forEach(v => {
    rowArr.push(
      ROW_NAME.map(k => v[k])
    )
  })

  resultArr[0] = {
    data: rowArr,
    name: 'sheet1',    // Excel 工作表名
  }

  const option = {'!cols': [{wch: 10}, {wch: 20}]}    // 自定义列宽度
  const buffer = xlsx.build(resultArr, option)
  return fs.writeFile(TMP_FILE_NAME, buffer, err => {
    if (err) {
      console.log('创建 Excel 导出文件失败')
      throw new Error(err)
    }
  })
}

/**
 * 上传文件
 */
function uploadFile() {
  let MyFile = new BaaS.File()
  return MyFile.upload(TMP_FILE_NAME, {category_id: EXPORT_DATA_CATEGORY_ID})
    .catch(err => {
      console.log('上传文件失败')
      throw new Error(err)
    })
}

module.exports = async function(event, callback) {
  try {
    const date = new Date().getTime()
    const groupInfoArr = []
    const groupInfoSplitArr = []
    const [jobId, totalCount] = await Promise.all([createExportJobIdRecord(TABLE_ID.export_task), getTotalCount(TABLE_ID.order)])
    const groupSize = Math.ceil(totalCount / LIMIT) || 1

    for (let i = 0; i < groupSize; i++) {
      groupInfoArr.push({
        offset: i * LIMIT,
        limit: LIMIT,
      })
    }

    console.log('groupInfoArr:', groupInfoArr)

    const length = Math.ceil(groupInfoArr.length / MAX_CONNECT_LIMIT)

    for (let i = 0; i < length; i++) {
      groupInfoSplitArr.push(groupInfoArr.splice(0, MAX_CONNECT_LIMIT))
    }

    console.log('groupInfoSplitArr:', groupInfoSplitArr)

    const date0 = new Date().getTime()
    console.log('处理分组情况耗时:', date0 - date, 'ms')

    let num = 0

    // 分批获取数据
    const getSplitDataList = index => {
      return Promise.all(
        groupInfoSplitArr[index].map(v => {
          return getDataByGroup(TABLE_ID.order, v.offset, v.limit)
        })
      ).then(res => {
        ++num
        result.push(...Array.prototype.concat(...res))
        if (num < groupInfoSplitArr.length) {
          return getSplitDataList(num)
        } else {
          return result
        }
      })
    }

    Promise.all([getSplitDataList(num)]).then(res => {
      const date1 = new Date().getTime()
      console.log('结果条数:', result.length)
      console.log('分组拉取数据次数:', num)
      console.log('拉取数据耗时:', date1 - date0, 'ms')

      genExportFile(result)

      const date2 = new Date().getTime()
      console.log('处理数据耗时:', date2 - date1, 'ms')

      uploadFile().then(res => {
        const fileLink = res.data.file_link
        const date3 = new Date().getTime()
        console.log('上传文件耗时:', date3 - date2, 'ms')
        console.log('总耗时:', date3 - date, 'ms')

        updateExportJobIdRecord(TABLE_ID.export_task, jobId, fileLink)
          .then(() => {
            const date4 = new Date().getTime()
            console.log('保存文件下载地址耗时:', date4 - date3, 'ms')
            console.log('总耗时:', date4 - date, 'ms')

            callback(null, {
              message: '保存文件下载地址成功',
              fileLink,
            })
          })
          .catch(err => {
            callback(err)
          })
      }).catch(err => {
        console.log('上传文件失败：', err)
        throw new Error(err)
      })
    })
  } catch (err) {
    callback(err)
  }
}
