-Bismillah


    pkg update -y
    
    pkg install golang git -y

    git clone https://github.com/km2262488/waf_test.git
    
    cd waf_test

    go run waf_test.go -url https://www.myweb.com -proxy-url LINK
