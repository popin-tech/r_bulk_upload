<?php
class Discovery{

    //public $aid;
    public $token;
    public $start_date;
    public $end_date;
    public $adList;
    public $camData;

    public function __construct($token = '', $sdate = '', $edate = '')
    {
        $this->token = $token;
        $this->start_date = $sdate;
        $this->end_date = $edate;
        //$this->main();
    }
    
    public function main(){
        //$this->aid = preg_replace("/[^0-9]/", "", $this->aid);
        $baseToken = base64_encode($this->token); //$this->get_base_token_by_aid($this->aid);
// $startTime = time();
        $accessToken = $this->getAccessToken($baseToken);
        // print_r($accessToken);exit;
// $periodtime = time() - $startTime; $startTime = time(); echo "抓access token：". intval($periodtime/60). ":". ($periodtime % 60). "<br>\n";
        $this->camData = $this->getCampaignData($accessToken);
// echo "camData:<br>\n";print_r($this->camData); exit;
// $periodtime = time() - $startTime; $startTime = time(); echo "抓 campaign data ：". intval($periodtime/60). ":". ($periodtime % 60). "<br>\n";
        $tmpCamAry = [];
        foreach($this->camData as $key => $cam){
            $cam_end_date_plus_1month = strtotime($cam['end_date']);
            $cam_end_date_plus_1month = strtotime('+1 month', $cam_end_date_plus_1month);
            if($this->token == 'YWNjb3VudF8xMjk3X2dyZXlzYQ='){
                if(!in_array($cam['mongo_id'], array(3068992 // special for 格蕾莎(Campaign太多了…會timeout) 把要抓廣告的cma id list 放這邊
                ,3666107
                ,3069077
                ,3796560
                ,3788317
                ,3793856
                ,3847641
                ,3847534
                ,3847468))){
                    continue;
                }
            }else{
                if(strtotime($this->start_date) > $cam_end_date_plus_1month){ // special for 格蕾莎(Campaign太多了…會timeout) || $cam['status'] == 0
                    continue;
                }    
            }
            
            $tmpCamAry[$cam['mongo_id']] = $cam;
        }
        $this->camData = $tmpCamAry;
        // print_r($this->camData);exit;

        $adListApiurls = [];
        foreach($this->camData as $key => $cam){
            $adListApiurls[] = "https://s2s.popin.cc/discovery/api/v2/ad/{$cam['mongo_id']}/lists";
        }

        $adList = $this->getAdData($accessToken, $adListApiurls);
// echo "adList:<br>\n"; print_r($adList); exit;
// $periodtime = time() - $startTime; $startTime = time(); echo "抓 ad data ：". intval($periodtime/60). ":". ($periodtime % 60). "<br>\n";
        $adListTmp = [];
        $reportListApiurls = [];
        foreach($adList as $key => $ad){
            $reportListApiurls[] = "https://s2s.popin.cc/discovery/api/v2/ad/{$ad['campaign']}/{$ad['mongo_id']}/{$this->start_date}/{$this->end_date}/date_reporting";
            $adListTmp[$ad['mongo_id']] = $ad;
        }
        $this->adList = $adListTmp;
        $reportData = $this->getReportData($accessToken, $reportListApiurls);
// echo "reportData:<br>\n"; print_r($reportData); exit;
// $periodtime = time() - $startTime; $startTime = time(); echo "抓 report data ：". intval($periodtime/60). ":". ($periodtime % 60). "<br>\n";
// exit;
        return $reportData;
    }

    public function get_ad_data(){
        $baseToken = base64_encode($this->token);
        $accessToken = $this->getAccessToken($baseToken);
        $this->camData = $this->getCampaignData($accessToken);
        $tmpCamAry = [];
        foreach($this->camData as $key => $cam){
            $tmpCamAry[$cam['mongo_id']] = $cam;
        }
        $this->camData = $tmpCamAry;

        $adListApiurls = [];
        foreach($this->camData as $key => $cam){
            $adListApiurls[] = "https://s2s.popin.cc/discovery/api/v2/ad/{$cam['mongo_id']}/lists";
        }

        $adList = $this->getAdData($accessToken, $adListApiurls);
        return $adList;
    }

    function get_base_token_by_aid($aid){
        $baseToken = '';
        try {
            $conn = (new Database)->dbConnect();
            $stmt = mysqli_prepare($conn, "SELECT * FROM `dctool_token_list` WHERE account_id = ?");
            if (!$stmt) {
                exit("準備查詢失敗: " . mysqli_error($conn));
            }
            mysqli_stmt_bind_param($stmt, "s", $aid);
            mysqli_stmt_execute($stmt);
            $result = mysqli_stmt_get_result($stmt);
            if (mysqli_num_rows($result) > 0) {
                while($row = mysqli_fetch_assoc($result)) {
                    //echo "account_id: " . $row["account_id"]. " - Name: " . $row["account_name"]. ", Token: " . $row["Token"] . "<br>";
                    $baseToken = base64_encode($row["Token"]);
                    if(!empty($baseToken)){
                        break;
                    }
                }
            } else {
                echo "沒有找到任何資料";
                return json_encode(array('status'=> false, 'msg'=> '沒有找到任何資料'));
            }
            mysqli_stmt_close($stmt);
            mysqli_close($conn);
        } catch(PDOException $e) {
            echo "連線失敗: " . $e->getMessage();
        }
        $conn = null;
        return $baseToken;
    }

    function curlMultiRequest($urls, $headers, $method = 'GET', $batchSize = 3) {
        $responses = [];
        $urlBatches = array_chunk($urls, $batchSize);

        foreach ($urlBatches as $urlBatch) {
            $batchResponses = $this->curlMultiRequestBatch($urlBatch, $headers, $method);
            $responses = array_merge($responses, $batchResponses);
            $interval = 1000000 / $batchSize;
            $remainingTime = 1000000 - $interval * count($urlBatch);
            if ($remainingTime > 0) {
                usleep((int)$remainingTime);
            }
        }
        return $responses;
    }

    function getAdReportCurlMultiRequest($urls, $headers, $method = 'GET', $batchSize = 3) {
        $responses = [];
        $urlBatches = array_chunk($urls, $batchSize);

        foreach ($urlBatches as $urlBatch) {
            $batchResponses = $this->curlMultiRequestBatch($urlBatch, $headers, $method);
            $responses = array_merge($responses, $batchResponses);
            $interval = 1000000 / $batchSize;
            $remainingTime = 1000000 - $interval * count($urlBatch);
            if ($remainingTime > 0) {
                usleep((int)$remainingTime);
            }
        }
        // print_r($responses);exit;
        return $responses;
    }

    function curlMultiRequestBatch($urls, $headers, $method = 'GET', $maxRetries = 3) {
        $curlHandles = [];
        $multiHandle = curl_multi_init();
        $retryUrls = [];
        $retryCounts = [];
        $responses = [];

        $backtrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 2);
        $callerFunction = isset($backtrace[1]['function']) ? $backtrace[1]['function'] : null;

        foreach ($urls as $url) {
            $handle = curl_init();
            curl_setopt_array($handle, [
                CURLOPT_URL => $url,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_HTTPHEADER => $headers,
                CURLOPT_CUSTOMREQUEST => $method,
                CURLOPT_TIMEOUT => 100, // 设置最大执行时间（秒）
                CURLOPT_CONNECTTIMEOUT => 30, // 设置连接超时时间（秒）
            ]);

            curl_multi_add_handle($multiHandle, $handle);
            $curlHandles[] = $handle;
            $retryCounts[$url] = 0;
        }

        do {
            $mrc = curl_multi_exec($multiHandle, $active);
        } while ($mrc == CURLM_CALL_MULTI_PERFORM);

        while ($active && $mrc == CURLM_OK) {
            if (curl_multi_select($multiHandle) != -1) {
                do {
                    $mrc = curl_multi_exec($multiHandle, $active);
                } while ($mrc == CURLM_CALL_MULTI_PERFORM);
            }
        }

        // 執行所有請求
        // $active = null;
        // do {
        //     curl_multi_exec($multiHandle, $active);
        //     curl_multi_select($multiHandle, 0.1); // 短暫等待，避免 CPU 過載
        // } while ($active > 0);

        foreach ($curlHandles as $index => $handle) {
            $content = curl_multi_getcontent($handle);
            $info = curl_getinfo($handle);
            $respontse = $tmpContent = [];

            if (strpos($content, '"code":1') !== false && strpos($content, '"msg":"ReportFlowLimit.operateTooMuch"') !== false) {
                if ($retryCounts[$info['url']] < $maxRetries) {
                    $retryUrls[] = $info['url'];
                    $retryCounts[$info['url']]++;
                } else {
                    //$responses[] = $content;
                    $tmpContent = $content;  // for d ad report
                }
            } else {
                //$responses[] = $content;
                $tmpContent = $content;  // for d ad report 
            }

            if (($callerFunction === 'getAdReportCurlMultiRequest' || preg_match('/\bad\b.*\bdate_reporting\b/', $info['url']))&& isset($tmpContent) && !empty($tmpContent)) {
                $pattern = '#/ad/(\d+)/(\d+)#';
                preg_match($pattern, $info['url'], $matches);

                $camId = empty($matches[1]) ? '' : $matches[1];
                $adId = empty($matches[2]) ? '' : $matches[2];

                $tmp = [];
                // echo $tmpContent. PHP_EOL;
                $resAry = json_decode($tmpContent, true);

                
                // if($callerFunction === 'getAdReportCurlMultiRequest') print_r($resAry);
                if(!empty($resAry['data'])){

                    // echo "\n\n\n[URL][ad id: {$adId}] {$info['url']}\n"; 
                    // if(empty($this->adList[$adId])){
                    //     echo "[EMPTY Ad Data]\n\n";
                    // }else{
                    //     print_r($this->adList[$adId]);
                    // }
                    // print_r($resAry);

                    foreach($resAry['data'] as $dataKey => $dataVal){
                        $resAry['data'][$dataKey] = array(
                                'account_name'=> $this->camData[$camId]['account'],
                                'campaign_name'=> $this->camData[$camId]['name']
                            )
                            + array(
                                'ad_title' => $this->adList[$adId]['title'],
                                'ad_image' => $this->adList[$adId]['image']
                            )
                            + $dataVal;
                    }
                    // echo "[After +acctont data + campaign data]\n"; print_r($resAry['data']);
                }
                $tmpContent = json_encode($resAry);
            }
            if(!empty($tmpContent)){
                $responses[] = $tmpContent;
                // echo "[every responses]\n"; print_r($responses);
            }
            unset($tmpContent);

            curl_multi_remove_handle($multiHandle, $handle);
            curl_close($handle);
        }

        curl_multi_close($multiHandle);

        if (!empty($retryUrls)) {
            // echo "[Retry]\n";
            $retryResponses = $this->curlMultiRequest($retryUrls, $headers, $method);
            $responses = array_merge($responses, $retryResponses);
        }
// echo "responses: \n";
// print_r($responses);
        return $responses;
    }

    function getAccessToken($basicToken) {
        $response = $this->curlMultiRequest(
            ['https://s2s.popin.cc/data/v1/authentication'],
            [
                'Content-Type: application/x-www-form-urlencoded; charset=utf-8',
                'Authorization: Basic ' . $basicToken,
                'Content-Length: 0'
            ],
            'POST'
        );

        return json_decode($response[0])->access_token;
    }

    function getCampaignData($accessToken) {
        $response = $this->curlMultiRequest(
            ['https://s2s.popin.cc/discovery/api/v2/campaign/lists?country_id=tw'],
            ['Authorization: Bearer ' . $accessToken]
        );

        $responseData = json_decode($response[0], true);

        return isset($responseData['data']) ? $responseData['data'] : [];
    }

    function getAdData($accessToken, $apiUrls) {
        $response = $this->curlMultiRequest(
            $apiUrls,
            ['Authorization: Bearer ' . $accessToken]
        );
        $responseData = [];
        foreach($response as $key => $res){
            $res2 = json_decode($res, true);
            $responseData = array_merge($responseData, isset($res2['data']) ? $res2['data'] : []);
        }
        return (isset($responseData) && !empty($responseData)) ? $responseData : [];
    }

    function getReportData($accessToken, $apiUrls) {
        $response = $this->getAdReportCurlMultiRequest(
            $apiUrls,
            ['Authorization: Bearer ' . $accessToken],
            "GET",
            3
        );
        // echo "[flag 1]\n"; print_r($response); exit;
        $response = $response ?? [];
        $resData = [];
        foreach($response as $res){
            $obj = json_decode($res, true);
            if(!empty($obj['data'])){
                if(is_array($obj['data'])){
                    foreach($obj['data'] as $data){
                        $resData[] = $data;
                    }
                }else{
                    $resData[] = $obj['data'];
                }
            }
        }
        return $resData;
    }
}