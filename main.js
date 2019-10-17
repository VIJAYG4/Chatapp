
$(document).ready(function() {
    var user = "user";
    document.getElementsByTagName("textarea")[0].innerHTML +=('\nBot : Hi there, how can I help?');
});

function pressEnter(ele) 
{
    if(event.key === 'Enter') {
        helper(document.getElementsByTagName('input')[0].value);    
    }	
}


function helper(input){
	document.getElementsByTagName("textarea")[0] = txtarea
    document.getElementsByTagName("input")[0].value = ""
	var m = "";
	input = input.toLowerCase()
    if (input == "" || null){
    	alert("Need Input");
    	return;
    }
    else{
    	document.getElementsByTagName("textarea")[0].innerHTML += ('\nUser: ' + input);
        lexResponse(input);
        txtarea.scrollTop = txtarea.scrollHeight;
        return;	
    }
}

function lexResponse(input){
	var apigClient = apigClientFactory.newClient();
	let params = {};
	let additionalParams = {};
	var body = {
		"message" : input
	};
	// apigClient = apigClientFactory.newClient({
 //      accessKey: AKIAJPEO2ZZBASWXMG5Q,
 //      secretKey: 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,
 //      region: 'us-east-1'
 // 	});
	apigClient.chatbotPost(params,body,additionalParams)
	.then(function(result){
		input = result.data.body;
		document.getElementsByTagName("textarea")[0].innerHTML += ('\nBot : ' + input);
        document.getElementsByTagName("textarea")[0].scrollTop = txtarea.scrollHeight;
	}).catch(function(result) {
       	// Add error callback code here.
        console.log(result);
    });
}
