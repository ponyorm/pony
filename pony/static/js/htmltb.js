$(function(){
    $("#system-hide").hide()
    system = $(".module-system")
    if(system.length == 0) {
        $("#system-show").hide()
    } else {
        system.hide()
    }
    
    $("#system-show").click(function(){
        $("#system-show").hide()
        $("#system-hide").show()
        $(".module-system").show("slow")
        return false
    })

    $("#system-hide").click(function(){
        $("#system-hide").hide()
        $("#system-show").show()
        $(".module-system").hide("slow")
        return false
    })
})