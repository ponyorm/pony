/**
* jQuery.smoothDivScroll - Smooth div scrolling using jQuery.
* This plugin is for turning a set of HTML elements's into a smooth scrolling area.
*
* Copyright (c) 2009 Thomas Kahn - thomas.kahn(at)karnhuset(dot)net
*
* This plugin is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* any later version.
*
* This plugin is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details. <http://www.gnu.org/licenses/>.
*
* Date: 2009-07-05
* @author Thomas Kahn
* @version 0.9
*
* Changelog
* ---------------------------------------------
* 0.9	- Bugfixes: Problem with multiple autoscrollers on the same page - the intervals
*		  where global which resulted in the wrong autoscroller stopping on mouseOver or
*		  mouseDown.
*		  Error in calculation in autoscrolling mode that made the autoscrolling grind
*		  to a halt after a number of loops.
*
* 0.8   - Major update. New parameter setup. Lots of new autoscrolling capabilities and 
*		  new parameters for controlling the scrolling speed. Made it possible to start 
*		  the scroller at a specific element.
* 
* 0.7   - Added support for autoscrolling after the page has loaded. 
*         Added support for making the hot spots visible at start for X number of seconds
*         or visible all the time.
*
* 0.6   - First version.
*/

(function($){jQuery.fn.smoothDivScroll=function(options){var defaults={scrollingHotSpotLeft:"div.scrollingHotSpotLeft",scrollingHotSpotRight:"div.scrollingHotSpotRight",scrollWrapper:"div.scrollWrapper",scrollableArea:"div.scrollableArea",hiddenOnStart:false,ajaxContentURL:"",countOnlyClass:"",scrollingSpeed:25,mouseDownSpeedBooster:3,autoScroll:"",autoScrollDirection:"right",autoScrollSpeed:1,pauseAutoScroll:"",visibleHotSpots:"",hotSpotsVisibleTime:5,startAtElementId:""};options=$.extend(defaults,options);return this.each(function(){var $mom=$(this);if(options.ajaxContentURL.length!==0){$mom.scrollableAreaWidth=0;$mom.find(options.scrollableArea).load((options.ajaxContentURL),function(){$mom.find(options.scrollableArea).children((options.countOnlyClass)).each(function(){$mom.scrollableAreaWidth=$mom.scrollableAreaWidth+$(this).outerWidth(true);});$mom.find(options.scrollableArea).css("width",($mom.scrollableAreaWidth+"px"));if(options.hiddenOnStart){$mom.hide();}
windowIsResized();setHotSpotHeightForIE();});}
var scrollXpos;var booster;var motherElementOffset=$mom.offset().left;var hotSpotWidth=0;booster=1;var hasExtended=false;$(window).one("load",function(){if(options.ajaxContentURL.length===0){$mom.scrollableAreaWidth=0;$mom.tempStartingPosition=0;$mom.find(options.scrollableArea).children((options.countOnlyClass)).each(function(){if((options.startAtElementId.length!==0)&&(($(this).attr("id"))==options.startAtElementId)){$mom.tempStartingPosition=$mom.scrollableAreaWidth;}
$mom.scrollableAreaWidth=$mom.scrollableAreaWidth+$(this).outerWidth(true);});$mom.find(options.scrollableArea).css("width",$mom.scrollableAreaWidth+"px");if(options.hiddenOnStart){$mom.hide();}}
$mom.find(options.scrollWrapper).scrollLeft($mom.tempStartingPosition);if(options.autoScroll!==""){$mom.autoScrollInterval=setInterval(autoScroll,6);}
if(options.autoScroll=="always")
{hideLeftHotSpot();hideRightHotSpot();}
switch(options.visibleHotSpots)
{case"always":makeHotSpotBackgroundsVisible();break;case"onstart":makeHotSpotBackgroundsVisible();$mom.hideHotSpotBackgroundsInterval=setInterval(hideHotSpotBackgrounds,(options.hotSpotsVisibleTime*1000));break;default:break;}});$mom.find(options.scrollingHotSpotRight,options.scrollingHotSpotLeft).one('mouseover',function(){if(options.autoScroll=="onstart"){clearInterval($mom.autoScrollInterval);}});$(window).bind("resize",function(){windowIsResized();});function windowIsResized(){if(!(options.hiddenOnStart))
{$mom.scrollableAreaWidth=0;$mom.find(options.scrollableArea).children((options.countOnlyClass)).each(function(){$mom.scrollableAreaWidth=$mom.scrollableAreaWidth+$(this).outerWidth(true);});$mom.find(options.scrollableArea).css("width",$mom.scrollableAreaWidth+'px');}
$mom.find(options.scrollWrapper).scrollLeft("0");var bodyWidth=$("body").innerWidth();if(options.autoScroll!=="always")
{if($mom.scrollableAreaWidth<bodyWidth)
{hideLeftHotSpot();hideRightHotSpot();}
else
{showHideHotSpots();}}}
function hideLeftHotSpot(){$mom.find(options.scrollingHotSpotLeft).hide();}
function hideRightHotSpot(){$mom.find(options.scrollingHotSpotRight).hide();}
function showLeftHotSpot(){$mom.find(options.scrollingHotSpotLeft).show();if(hotSpotWidth<=0){hotSpotWidth=$mom.find(options.scrollingHotSpotLeft).width();}}
function showRightHotSpot(){$mom.find(options.scrollingHotSpotRight).show();if(hotSpotWidth<=0){hotSpotWidth=$mom.find(options.scrollingHotSpotRight).width();}}
function setHotSpotHeightForIE()
{jQuery.each(jQuery.browser,function(i,val){if(i=="msie"&&jQuery.browser.version.substr(0,1)=="6")
{$mom.find(options.scrollingHotSpotLeft).css("height",($mom.find(options.scrollableArea).innerHeight()));$mom.find(options.scrollingHotSpotRight).css("height",($mom.find(options.scrollableArea).innerHeight()));}});}
$mom.find(options.scrollingHotSpotRight).bind('mousemove',function(e){var x=e.pageX-(this.offsetLeft+motherElementOffset);scrollXpos=Math.round((x/hotSpotWidth)*options.scrollingSpeed);if(scrollXpos===Infinity){scrollXpos=0;}});$mom.find(options.scrollingHotSpotRight).bind('mouseover',function(){if(options.autoScroll=="onstart"){clearInterval($mom.autoScrollInterval);}
$mom.rightScrollInterval=setInterval(doScrollRight,6);});$mom.find(options.scrollingHotSpotRight).bind('mouseout',function(){clearInterval($mom.rightScrollInterval);scrollXpos=0;});$mom.find(options.scrollingHotSpotRight).bind('mousedown',function(){booster=options.mouseDownSpeedBooster;});$("*").bind('mouseup',function(){booster=1;});var doScrollRight=function()
{if(scrollXpos>0){$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft()+(scrollXpos*booster));}
showHideHotSpots();};if(options.pauseAutoScroll=="mousedown"&&options.autoScroll=="always")
{$mom.find(options.scrollWrapper).bind('mousedown',function(){clearInterval($mom.autoScrollInterval);});$mom.find(options.scrollWrapper).bind('mouseup',function(){$mom.autoScrollInterval=setInterval(autoScroll,6);});}
else if(options.pauseAutoScroll=="mouseover"&&options.autoScroll=="always")
{$mom.find(options.scrollWrapper).bind('mouseover',function(){clearInterval($mom.autoScrollInterval);});$mom.find(options.scrollWrapper).bind('mouseout',function(){$mom.autoScrollInterval=setInterval(autoScroll,6);});}
$mom.previousScrollLeft=0;$mom.pingPongDirection="right";$mom.swapAt;$mom.getNextElementWidth=true;var autoScroll=function()
{if(options.autoScroll=="onstart"){showHideHotSpots();}
switch(options.autoScrollDirection)
{case"right":$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft()+options.autoScrollSpeed);break;case"left":$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft()-options.autoScrollSpeed);break;case"backandforth":$mom.previousScrollLeft=$mom.find(options.scrollWrapper).scrollLeft();if($mom.pingPongDirection=="right"){$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft()+options.autoScrollSpeed);}
else{$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft()-options.autoScrollSpeed);}
if($mom.previousScrollLeft===$mom.find(options.scrollWrapper).scrollLeft())
{if($mom.pingPongDirection=="right"){$mom.pingPongDirection="left";}
else{$mom.pingPongDirection="right";}}
break;case"endlessloop":if($mom.getNextElementWidth)
{if(options.startAtElementId!==""){$mom.swapAt=$("#"+options.startAtElementId).outerWidth();}
else{$mom.swapAt=$mom.find(options.scrollableArea).children(":first-child").outerWidth();}
$mom.getNextElementWidth=false;}
$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft()+options.autoScrollSpeed);if(($mom.swapAt<=$mom.find(options.scrollWrapper).scrollLeft()))
{$mom.find(options.scrollableArea).append($mom.find(options.scrollableArea).children(":first-child").clone());$mom.find(options.scrollWrapper).scrollLeft(($mom.find(options.scrollWrapper).scrollLeft()-$mom.find(options.scrollableArea).children(":first-child").outerWidth()));$mom.find(options.scrollableArea).children(":first-child").remove();$mom.getNextElementWidth=true;}
break;default:break;}};$mom.find(options.scrollingHotSpotLeft).bind('mousemove',function(e){var x=$mom.find(options.scrollingHotSpotLeft).innerWidth()-(e.pageX-motherElementOffset);scrollXpos=Math.round((x/hotSpotWidth)*options.scrollingSpeed);if(scrollXpos===Infinity)
{scrollXpos=0;}});$mom.find(options.scrollingHotSpotLeft).bind('mouseover',function(){if(options.autoScroll=="onstart"){clearInterval($mom.autoScrollInterval);}
$mom.leftScrollInterval=setInterval(doScrollLeft,6);});$mom.find(options.scrollingHotSpotLeft).bind('mouseout',function(){clearInterval($mom.leftScrollInterval);scrollXpos=0;});$mom.find(options.scrollingHotSpotLeft).bind('mousedown',function(){booster=options.mouseDownSpeedBooster;});var doScrollLeft=function()
{if(scrollXpos>0){$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft()-(scrollXpos*booster));}
showHideHotSpots();};function showHideHotSpots()
{if($mom.find(options.scrollWrapper).scrollLeft()===0)
{hideLeftHotSpot();showRightHotSpot();}
else if(($mom.scrollableAreaWidth)<=($mom.find(options.scrollWrapper).innerWidth()+$mom.find(options.scrollWrapper).scrollLeft()))
{hideRightHotSpot();showLeftHotSpot();}
else
{showRightHotSpot();showLeftHotSpot();}}
function makeHotSpotBackgroundsVisible()
{$mom.find(options.scrollingHotSpotLeft).addClass("scrollingHotSpotLeftVisible");$mom.find(options.scrollingHotSpotRight).addClass("scrollingHotSpotRightVisible");}
function hideHotSpotBackgrounds()
{clearInterval($mom.hideHotSpotBackgroundsInterval);$mom.find(options.scrollingHotSpotLeft).fadeTo("slow",0.0,function(){$mom.find(options.scrollingHotSpotLeft).removeClass("scrollingHotSpotLeftVisible");});$mom.find(options.scrollingHotSpotRight).fadeTo("slow",0.0,function(){$mom.find(options.scrollingHotSpotRight).removeClass("scrollingHotSpotRightVisible");});}});};})(jQuery);