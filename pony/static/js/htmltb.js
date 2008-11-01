$(function(){
    $("#system-hide").hide()
    system = $(".system")
    if(system.length == 0) {
        $("#system-show").hide()
    } else {
        system.hide()
    }
    
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