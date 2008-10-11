$(function(){
    $("#system-hide").hide()
    $(".system").hide()
    
    $("#system-show").click(function(){
        $("#system-show").hide()
        $("#system-hide").show()
        $(".system").show("slow")
        return false
    })

    $("#system-hide").click(function(){
        $("#system-hide").hide()
        $("#system-show").show()
        $(".system").hide("slow")
        return false
    })
})