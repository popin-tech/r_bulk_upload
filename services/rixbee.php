<?php
class Rixbee {
    public $rixbeeToken;
    public $rixbeeAccountType;
    public $rixbeeUserId;
    public $rixbeeAccountIds;
    public $startDate;
    public $endDate;

    public function __construct($aid, $rixbeeAccountType, $startDate, $endDate){
        $this->rixbeeToken = 'f3c1b67f25e4423001cd9a29fb310998';
        $this->rixbeeUserId = '7161';
        if (isset($rixbeeAccountType) && $rixbeeAccountType === 'direct') {
            $this->rixbeeToken = 'f3f63d0b878569c7b824096b1a0f14b2';
            $this->rixbeeUserId = '7168';
        }else if(isset($rixbeeAccountType) && $rixbeeAccountType === 'super'){
            /******* super token  ********/
            $this->rixbeeToken = 'e36da40d2fe00d708464c0269c051140';
            $this->rixbeeUserId = '7153';
            /******* super token  ********/
        }
        
        $this->rixbeeAccountIds = $aid;
        $this->startDate = $startDate;
        $this->endDate = $endDate;
    }

    function getRixbeeData(){
        $accountIds = explode(',', $this->rixbeeAccountIds);
        $accountString = implode('', array_map(function ($accountId) {
            return '&user_id[]=' . $accountId;
        }, $accountIds));

        $start = new DateTime($this->startDate);
        $end = new DateTime($this->endDate);
        // $end->modify('+1 day');

        $dateRange = new DatePeriod($start, new DateInterval('P1D'), $end);

        $multiHandle = curl_multi_init();
        $curlHandles = [];

        $rixBeeDeviceData = [];
        $multiHandleDevice = curl_multi_init();
        $curlHandlesDevice = [];

        $endForDatePeriod = clone $end;
        $endForDatePeriod->modify('+1 day');
        $dateRange = new DatePeriod($start, new DateInterval('P7D'), $endForDatePeriod);
        $dates = iterator_to_array($dateRange);
        for ($i = 0; $i < count($dates); $i++) {
            $currentStart = $dates[$i]->format('Y-m-d');
            if (isset($dates[$i + 1])) {
                $weekEnd = clone $dates[$i + 1];
                $weekEnd->modify('-1 day');
                $currentEnd = ($weekEnd > $end) ? $end : $weekEnd;
            } else {
                $currentEnd = $end;
            }
            $currentEnd = $currentEnd->format('Y-m-d');
            $curlHandles[$currentStart] = curl_init();
            $url = 'https://broadciel.rpt.rixbeedesk.com/api/report/v1?x-userid=' . $this->rixbeeUserId . '&x-authorization=' . $this->rixbeeToken . '&start_date=' . $currentStart . '&end_date=' . $currentEnd . '&timezone=UTC+8&dimensions[]=day&dimensions[]=country&dimensions[]=group_id&dimensions[]=cr_id&dimensions[]=cpg_id&dimensions[]=ad_channel&dimensions[]=ad_target&currency=TWD' . $accountString;
            curl_setopt($curlHandles[$currentStart], CURLOPT_URL, $url);
            curl_setopt($curlHandles[$currentStart], CURLOPT_RETURNTRANSFER, true);
            curl_multi_add_handle($multiHandle, $curlHandles[$currentStart]);
        }

        // foreach ($dateRange as $date) {
        //     $nowDate = $date->format('Y-m-d');

        //     $curlHandles[$nowDate] = curl_init();
        //     $url = 'https://broadciel.rpt.rixbeedesk.com/api/report/v1?x-userid=' . $this->rixbeeUserId . '&x-authorization=' . $this->rixbeeToken . '&start_date=' . $nowDate . '&end_date=' . $nowDate . '&timezone=UTC+8&dimensions[]=day&dimensions[]=country&dimensions[]=group_id&dimensions[]=cr_id&dimensions[]=cpg_id&dimensions[]=ad_channel&dimensions[]=ad_target&currency=TWD' . $accountString;
        //     curl_setopt($curlHandles[$nowDate], CURLOPT_URL, $url);
        //     curl_setopt($curlHandles[$nowDate], CURLOPT_RETURNTRANSFER, true);
        //     curl_multi_add_handle($multiHandle, $curlHandles[$nowDate]);

        //     /*
        //     $curl = curl_init();
        //     curl_setopt_array($curl, array(
        //         CURLOPT_URL => 'https://broadciel.rpt.rixbeedesk.com/api/report/v1?x-userid=' . $this->rixbeeUserId . '&x-authorization=' . $this->rixbeeToken . '&start_date=' . $nowDate . '&end_date=' . $nowDate . '&timezone=UTC+8&dimensions[]=day&dimensions[]=device_type&currency=TWD' . $accountString,
        //         CURLOPT_RETURNTRANSFER => true,
        //         CURLOPT_ENCODING => '',
        //         CURLOPT_MAXREDIRS => 10,
        //         CURLOPT_TIMEOUT => 0,
        //         CURLOPT_FOLLOWLOCATION => true,
        //         CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
        //         CURLOPT_CUSTOMREQUEST => 'GET',
        //     ));
        //     curl_multi_add_handle($multiHandleDevice, $curl);
        //     $curlHandlesDevice[$nowDate] = $curl;
        //     // */
        // }

        $errorMapping = array(
            '1000' => '非常抱歉，R API 異常，請再試一次，持續錯誤請通知你的IT',
            '1003' => '非常抱歉，R API 每日使用到達上限，明天再試(或通知你的IT)',
            '1001' => '非常抱歉，金鑰驗錯誤，持續錯誤請通知你的IT',
            '1002' => '非常抱歉，取得 R 報錯異常，請截圖通知你的IT',
            '1006' => '非常抱歉，系統資料異常，請截圖通知你的IT',
        );

        $running = null;
        do {
            curl_multi_exec($multiHandle, $running);
        } while ($running > 0);

        $rixBeeData = [];
        foreach ($dateRange as $date) {
            $nowDate = $date->format('Y-m-d');
            $response = curl_multi_getcontent($curlHandles[$nowDate]);
            $resAry = json_decode($response, true);
            // print_r($response);exit;
            if($resAry['status']['code'] != 0){
                
                if(in_array($resAry['status']['code'], $errorMapping)){
                    $errorMsg = $errorMapping[$resAry['status']['code']];
                }else{
                    $errorMsg = "非常抱歉，R API 出現異常，請截圖給你的IT協助處理。";
                }
                $errorMsg .= "\n {$resAry['status']['code']} {$resAry['status']['message']}";

                header('Content-Type: application/json; charset=utf-8');
                exit(json_encode(['errorMsg' => $errorMsg, 'errorConsole' => 'This is the R API error']));
            }
            $rixBeeData[$nowDate] = $resAry['data']['data'];
            curl_multi_remove_handle($multiHandle, $curlHandles[$nowDate]);
            curl_close($curlHandles[$nowDate]);
        }
        // print_r($rixBeeData);exit;

        /*
        do {
            curl_multi_exec($multiHandleDevice, $running);
        } while ($running > 0);
        
        foreach ($dateRange as $date) {
            $nowDate = $date->format('Y-m-d');
            $response = curl_multi_getcontent($curlHandlesDevice[$nowDate]);
            echo "(section B)<br>\n";
            print_r($response);
            $rixBeeDeviceData = array_merge($rixBeeDeviceData, json_decode($response, true)['data']['data']);
            curl_multi_remove_handle($multiHandleDevice, $curlHandlesDevice[$nowDate]);
            curl_close($curlHandlesDevice[$nowDate]);
        }
            */

        curl_multi_close($multiHandle);
        // curl_multi_close($multiHandleDevice);

        $fields = [
            'RBD報表欄位' => 'RBD報表欄位',
            'day' => 'Date',
            'group_name' => 'AdGroups',
            'cpg_id' => 'campaignid',
            'cr_name' => 'assetname',
            'cr_id' => 'assetid',
            'cr_title' => 'assettitle',
            'cr_image' => 'assetimage',
            'group_name' => 'groupname',
            'cpg_name' => 'AdAssets',
            'user_name' => 'brandname',
            'ad_target' => 'landingpage',
            'payment_revenue' => 'Spend',
            'impression' => 'Impressions',
            'click' => 'Clicks',
            'behavior1' => 'CompleteCheckout',
            'behavior4' => 'AddToCart',
            'behavior0' => 'ViewContent',
            'behavior2' => 'Checkout',
            'behavior3' => 'Bookmark',
            'behavior5' => 'Search',
            'behavior6' => 'CompleteRegistration',
        ];

        $result = [];
        foreach ($rixBeeData as $nowDate => $data) {
            foreach ($data as $item) {
                $filteredItem = [];
                foreach ($fields as $key => $newKey) {
                    $filteredItem['RBD報表欄位'] = 'RBD報表欄位';
                    if ($key === 'day' && isset($item[$key])) {
                        $filteredItem[$newKey] = str_replace('-', '', $item[$key]);
                    } elseif ($key === 'cpg_name') {
                        $filteredItem[$newKey] = $item['cr_name'];
                    } elseif (isset($item[$key])) {
                        $filteredItem[$newKey] = $item[$key];
                    }
                }
                $result[] = $filteredItem;
            }
        }

        /*
        $summarizedData = array_reduce($rixBeeDeviceData, function ($carry, $item) {
            $key = $item['day'] . '_' . $item['device_type'];
            $deviceMapping = [0 => 'Others', 1 => 'Mobile', 2 => 'Desktop', 3 => 'Others', 4 => 'Mobile', 5 => 'Mobile'];
            $device = array_key_exists($item['device_type'], $deviceMapping) ? $deviceMapping[$item['device_type']] : 'Mobile';

            if (!array_key_exists($key, $carry)) {
                $carry[$key] = [
                    '來源' => 'RBD',
                    'date' => str_replace('-', '', $item['day']),
                    'device' => $device,
                    'spend' => 0,
                    'impression' => 0,
                    'click' => 0,
                    'CompleteCheckout' => 0,
                    'AddToCart' => 0,
                    'ViewContent' => 0,
                    'Checkout' => 0,
                    'Bookmark' => 0,
                    'Search' => 0,
                    'CompleteRegistration' => 0,
                ];
            }

            $carry[$key]['spend'] += $item['payment_revenue'];
            $carry[$key]['impression'] += $item['impression'];
            $carry[$key]['click'] += $item['click'];
            $carry[$key]['CompleteCheckout'] += $item['behavior1'];
            $carry[$key]['AddToCart'] += $item['behavior4'];
            $carry[$key]['ViewContent'] += $item['behavior0'];
            $carry[$key]['Checkout'] += $item['behavior2'];
            $carry[$key]['Bookmark'] += $item['behavior3'];
            $carry[$key]['Search'] += $item['behavior5'];
            $carry[$key]['CompleteRegistration'] += $item['behavior6'];

            return $carry;
        }, []);

        $finalSummarizedData = array_values($summarizedData);
        */

        $response = [
            'rixbeeData' => $result,
            'rixbeeDeviceData' => array(), // $finalSummarizedData,
        ];

        return $response;
    }
}
