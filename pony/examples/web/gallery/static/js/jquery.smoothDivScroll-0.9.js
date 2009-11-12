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
* Date: 2009-05-23
* @author Thomas Kahn
* @version 0.8
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

(function($) { 
	jQuery.fn.smoothDivScroll = function(options){

		var defaults = {
		scrollingHotSpotLeft: "div.scrollingHotSpotLeft", // The hot spot that triggers scrolling left.
		scrollingHotSpotRight: "div.scrollingHotSpotRight", // The hot spot that triggers scrolling right.
		scrollWrapper: "div.scrollWrapper", // The wrapper element that surrounds the scrollable area
		scrollableArea: "div.scrollableArea", // The actual element that is scrolled left or right
		hiddenOnStart: false, // True or false. Determines whether the element should be visible or hidden on start
		ajaxContentURL: "", // Optional. If supplied, content is fetched through AJAX using the supplied URL
		countOnlyClass: "", // Optional. If supplied, the function that calculates the width of the scrollable area will only count elements of this class
		scrollingSpeed: 25, // A way of controlling the scrolling speed. 1=slowest and 100= fastest.
		mouseDownSpeedBooster: 3, // 1 is normal speed (no speed boost), 2 is twice as fast, 3 is three times as fast, and so on
		autoScroll: "", // Optional. Leave it blank if you don't want any autoscroll. 
						// Otherwise use the values "onstart" or "always". 
						// onstart - the scrolling will start automatically after 
						// the page has loaded and scroll according to the method you've selected 
						// using the autoScrollDirection parameter. When the user moves the mouse 
						// over the left or right hot spot the autoscroll will stop. After that 
						// the scrolling will only be triggered by the host spots.
						// always - the hot spots are disabled alltogether and the scrollable area 
						// will only scroll automatically.
		autoScrollDirection: "right", 	// This parameter controls the direction and behavior of the autoscrolling.	
										// Optional. The values are:
										// right - autoscrolls right and stops when it reaches the end
										// left - autoscrolls left and stops when it reaches the end 
										// (only relevant if you have set the parameter startAtElementId).
										// backandforth - starts autoscrolling right and when it reaches 
										// the end, switches to autoscrolling left and so on. Ping-pong style.
										// endlessloop - continuous scrolling right. An endless loop of elements.
		autoScrollSpeed: 1,	//  1-2 = slow, 3-4 = medium, 5-13 = fast -- anything higher = superfast
		pauseAutoScroll: "", // Optional. Values mousedown and mouseover. Leave blank for no pausing abilities.
		visibleHotSpots: "", 	// Optional. Leave it blank for invisible hot spots. 
								// Otherwise use the values  "onstart" or "always". 
								// onstart - makes the hot spots visible for X-number of seconds 
								// after tha page has loaded and then they become invisible. 
								// always - hot spots are visible all the time.
		hotSpotsVisibleTime: 5, // If you have selected "onstart" as the value for visibleHotSpots, 
								// you set the number of seconds that you want the hot spots to be 
								// visible after the page has loaded. After this time they will fade 
								// away and become invisible again.
		startAtElementId: ""	// Optional. Use this parameter if you want the offset of the 
								// scrollable area to be positioned at a specific element directly 
								// after the page has loaded. First give your element an ID in the 
								// HTML code and then provide this ID as a parameter.
		};

		options = $.extend(defaults, options);

		/* Identify global variables so JSLint won't raise errors when verifying the code */
		/*global autoScrollInterval, autoScroll, clearInterval, doScrollLeft, doScrollRight, hideHotSpotBackgrounds, hideHotSpotBackgroundsInterval, hideLeftHotSpot, hideRightHotSpot, jQuery, makeHotSpotBackgroundsVisible, setHotSpotHeightForIE, setInterval, showHideHotSpots, window, windowIsResized */


		// Iterate and make each matched element a SmoothDivScroll
		return this.each(function() {
		
			// Create a variable for the current "mother element"
			var $mom = $(this);
			
			// Load the content of the scrollable area using the optional URL.
			// If no ajaxContentURL is supplied, we assume that the content of
			// the scrolling area is already in place.
			if(options.ajaxContentURL.length !== 0){
				$mom.scrollableAreaWidth = 0;
				$mom.find(options.scrollableArea).load((options.ajaxContentURL), function(){	
					$mom.find(options.scrollableArea).children((options.countOnlyClass)).each(function() {
						$mom.scrollableAreaWidth = $mom.scrollableAreaWidth + $(this).outerWidth(true);
					});

					// Set the width of the scrollable area
					$mom.find(options.scrollableArea).css("width", ($mom.scrollableAreaWidth + "px"));
					
					// Hide the mother element if it shouldn't be visible on start
					if(options.hiddenOnStart) {
						$mom.hide();
					}
					
					windowIsResized();
					
					setHotSpotHeightForIE();
				});		
			}
			
			// Some variables used for working with the scrolling
			var scrollXpos;
			var booster;
			
			// The left offset of the container on which you place 
			// the scrolling behavior.
			// This offset is used when calculating the mouse x-position 
			// in relation to scroll hot spots
			var motherElementOffset = $mom.offset().left;
			
			// A variable used for storing the current hot spot width.
			// It is used when calculating the scroll speed
			var hotSpotWidth = 0;
			
			// Set the booster value to normal (doesn't change until the user
			// holds down the mouse button over one of the hot spots)
			booster = 1;
			
			var hasExtended = false;
			
			// Stuff to do once on load
			$(window).one("load",function(){
				// If the content of the scrolling area is not loaded through ajax,
				// we assume it's already there and can run the code to calculate
				// the width of the scrolling area, resize it to that width
				if(options.ajaxContentURL.length === 0) {
					$mom.scrollableAreaWidth = 0;
					$mom.tempStartingPosition = 0;
					
					$mom.find(options.scrollableArea).children((options.countOnlyClass)).each(function() {
						
						// Check to see if the current element in the loop is the one where the scrolling should start
						if( (options.startAtElementId.length !== 0) && (($(this).attr("id")) == options.startAtElementId) ) {
						$mom.tempStartingPosition = $mom.scrollableAreaWidth;
						}

						// Add the width of the current element in the loop to the total width
						$mom.scrollableAreaWidth = $mom.scrollableAreaWidth + $(this).outerWidth(true);
						
					});
					
					// Set the width of the scrollableArea to the accumulated width
					$mom.find(options.scrollableArea).css("width", $mom.scrollableAreaWidth + "px");
					
					// Check to see if the whole thing should be hidden at start
					if(options.hiddenOnStart) {
						$mom.hide();
					}
				}
				
				// Set the starting position of the scrollable area. If no startAtElementId is set, the starting position
				// will be the default value (zero)
				$mom.find(options.scrollWrapper).scrollLeft($mom.tempStartingPosition);
				
				// If the user has set the option autoScroll, the scollable area will
				// start scrolling automatically
				if(options.autoScroll !== "") {
					$mom.autoScrollInterval = setInterval(autoScroll, 6);
				}

				// If autoScroll is set to always, the hot spots should be disabled
				if(options.autoScroll == "always")
				{
					hideLeftHotSpot();
					hideRightHotSpot();
				}
	
				// If the user wants to have visible hot spots, here is where it's taken care of
				switch(options.visibleHotSpots)
				{
					case "always":
						makeHotSpotBackgroundsVisible();
						break;
					case "onstart":
						makeHotSpotBackgroundsVisible();
						$mom.hideHotSpotBackgroundsInterval = setInterval(hideHotSpotBackgrounds, (options.hotSpotsVisibleTime * 1000));
						break;
					default:
						break;	
				}
				
			});
			
			// If autoScroll is running, here's where it's stopped when the user positions the mouse over one of the hot spots
			$mom.find(options.scrollingHotSpotRight, options.scrollingHotSpotLeft).one('mouseover',function(){
				if(options.autoScroll == "onstart") {
					clearInterval($mom.autoScrollInterval);
				}
			});	

			
			// EVENT - window resize
			$(window).bind("resize",function(){
				windowIsResized();
			});

			// A function for doing the stuff that needs to be
			// done when the browser window is resized
			function windowIsResized() {
			
				// If the scrollable area is not hidden on start, reset and recalculate the
				// width of the scrollable area
				if(!(options.hiddenOnStart))
				{
					$mom.scrollableAreaWidth = 0;
					$mom.find(options.scrollableArea).children((options.countOnlyClass)).each(function() {
						$mom.scrollableAreaWidth = $mom.scrollableAreaWidth + $(this).outerWidth(true);
					});
					
					$mom.find(options.scrollableArea).css("width", $mom.scrollableAreaWidth + 'px');
				}

				// Reset the left offset of the scroll wrapper
				$mom.find(options.scrollWrapper).scrollLeft("0");
				
				// Get the width of the page (body)
				var bodyWidth = $("body").innerWidth();
				
				// If the scrollable area is shorter than the current
				// window width, both scroll hot spots should be hidden.
				// Otherwise, check which hot spots should be shown.
				if(options.autoScroll !== "always")
				{
					if($mom.scrollableAreaWidth < bodyWidth)
					{	
						hideLeftHotSpot();
						hideRightHotSpot();
					}
					else
					{
						showHideHotSpots();
					}
				}
			}
			
			// HELPER FUNCTIONS FOR SHOWING AND HIDING HOT SPOTS
			function hideLeftHotSpot(){
				$mom.find(options.scrollingHotSpotLeft).hide();
			}
			
			function hideRightHotSpot(){
				$mom.find(options.scrollingHotSpotRight).hide();
			}
			
			function showLeftHotSpot(){
				$mom.find(options.scrollingHotSpotLeft).show();
				// Recalculate the hot spot width. Do it here because you can
				// be sure that the hot spot is visible and has a width
				if(hotSpotWidth <= 0) {
					hotSpotWidth = $mom.find(options.scrollingHotSpotLeft).width();
				}
			}
			
			function showRightHotSpot(){
				$mom.find(options.scrollingHotSpotRight).show();
				// Recalculate the hot spot width. Do it here because you can
				// be sure that the hot spot is visible and has a width
				if(hotSpotWidth <= 0) {
					hotSpotWidth = $mom.find(options.scrollingHotSpotRight).width();
				}
			}
			
			function setHotSpotHeightForIE()
			{
				// Some bugfixing for IE 6
				jQuery.each(jQuery.browser, function(i, val) {
					if(i=="msie" && jQuery.browser.version.substr(0,1)=="6")
					{
						$mom.find(options.scrollingHotSpotLeft).css("height", ($mom.find(options.scrollableArea).innerHeight()));
						$mom.find(options.scrollingHotSpotRight).css("height", ($mom.find(options.scrollableArea).innerHeight()));				
					}
				});
			}
			// **************************************************
			// EVENTS - scroll right
			// **************************************************
			
			// Check the mouse X position and calculate the relative X position inside the right hot spot
			$mom.find(options.scrollingHotSpotRight).bind('mousemove',function(e){
				var x = e.pageX - (this.offsetLeft + motherElementOffset);
				scrollXpos = Math.round((x/hotSpotWidth) * options.scrollingSpeed);
				if(scrollXpos === Infinity) {
					scrollXpos = 0;
				}

			});

			// mouseover right hot spot
			$mom.find(options.scrollingHotSpotRight).bind('mouseover',function(){
				if(options.autoScroll == "onstart") {
					clearInterval($mom.autoScrollInterval);
				}
				$mom.rightScrollInterval = setInterval(doScrollRight, 6);
			});	
			
			// mouseout right hot spot
			$mom.find(options.scrollingHotSpotRight).bind('mouseout',function(){
				clearInterval($mom.rightScrollInterval);
				scrollXpos = 0;
			});
			
			// scrolling speed booster right
			$mom.find(options.scrollingHotSpotRight).bind('mousedown',function(){
				booster = options.mouseDownSpeedBooster;
			});
			
			// stop boosting the scrolling speed
			$("*").bind('mouseup',function(){
				booster = 1;
			});
	
			
			// The function that does the actual scrolling right
			var doScrollRight = function()
			{	
				if(scrollXpos > 0) {
					$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft() + (scrollXpos*booster));
				}
				showHideHotSpots();
			};
			
			// **************************************************
			// Autoscrolling
			// **************************************************

			if(options.pauseAutoScroll == "mousedown" && options.autoScroll == "always")
			{
				$mom.find(options.scrollWrapper).bind('mousedown',function(){
					clearInterval($mom.autoScrollInterval);
				});
				
				$mom.find(options.scrollWrapper).bind('mouseup',function(){
					$mom.autoScrollInterval = setInterval(autoScroll, 6);
				});
			}
			else if(options.pauseAutoScroll == "mouseover" && options.autoScroll == "always")
			{
				$mom.find(options.scrollWrapper).bind('mouseover',function(){
					clearInterval($mom.autoScrollInterval);
				});
				
				$mom.find(options.scrollWrapper).bind('mouseout',function(){
					$mom.autoScrollInterval = setInterval(autoScroll, 6);
				});
			}
			
			$mom.previousScrollLeft = 0;
			$mom.pingPongDirection = "right";
			$mom.swapAt;
			$mom.getNextElementWidth = true;
			// The autoScroll function
			var autoScroll = function()
			{	
				if (options.autoScroll == "onstart") {
					showHideHotSpots();
				}
				
				switch(options.autoScrollDirection)
				{
					case "right":
						$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft() + options.autoScrollSpeed);
						break;
						
					case "left":
						$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft() - options.autoScrollSpeed);
						break;
						
					case "backandforth":
						// Store the old scrollLeft value to see if the scrolling has reached the end
						$mom.previousScrollLeft = $mom.find(options.scrollWrapper).scrollLeft();
						
						if($mom.pingPongDirection == "right") {
							$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft() + options.autoScrollSpeed);
						}
						else {
							$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft() - options.autoScrollSpeed);
						}
						
						// If the scrollLeft hasnt't changed it means that the scrolling has reached
						// the end and the direction should be switched
						if($mom.previousScrollLeft === $mom.find(options.scrollWrapper).scrollLeft())
						{
							if($mom.pingPongDirection == "right") {
								$mom.pingPongDirection = "left";
							}
							else {
								$mom.pingPongDirection = "right";
							}
						}
						break;
		
					case "endlessloop":
						// Get the width of the first element. When it has scrolled out of view,
						// the element swapping should be executed. A true/false variable is used
						// as a flag variable so the swapAt value doesn't have to be recalculated
						// in each loop.
						if($mom.getNextElementWidth)
						{
							if(options.startAtElementId !== "") {
								$mom.swapAt = $("#" + options.startAtElementId).outerWidth();
							}
							else {
								$mom.swapAt = $mom.find(options.scrollableArea).children(":first-child").outerWidth();
							}
							
							$mom.getNextElementWidth = false;
						}
						
						// Do the autoscrolling
						$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft() + options.autoScrollSpeed);
						
						// Check to see if the swap should be done
						if(($mom.swapAt <= $mom.find(options.scrollWrapper).scrollLeft()))
						{ 
							// Clone the first element and append it last in the scrollableArea
							$mom.find(options.scrollableArea).append($mom.find(options.scrollableArea).children(":first-child").clone());

							// Compensate for the removal of the first element by
							$mom.find(options.scrollWrapper).scrollLeft(($mom.find(options.scrollWrapper).scrollLeft() - $mom.find(options.scrollableArea).children(":first-child").outerWidth()));
							
							// Remove it from its original position as the first element
							$mom.find(options.scrollableArea).children(":first-child").remove();
							
							$mom.getNextElementWidth = true;
						}
						break;
					default:
						break;
						
				}

			};
			
			
			// **************************************************
			// EVENTS - scroll left
			// **************************************************
		
			// Check the mouse X position and calculate the relative X position inside the left hot spot
			$mom.find(options.scrollingHotSpotLeft).bind('mousemove',function(e){
				var x = $mom.find(options.scrollingHotSpotLeft).innerWidth() - (e.pageX - motherElementOffset);
				scrollXpos = Math.round((x/hotSpotWidth) * options.scrollingSpeed);
				if(scrollXpos === Infinity)
				{
					scrollXpos = 0;
				}
			});
			
			// mouseover left hot spot
			$mom.find(options.scrollingHotSpotLeft).bind('mouseover',function(){
				if(options.autoScroll == "onstart") {
					clearInterval($mom.autoScrollInterval);
				}
				
				$mom.leftScrollInterval = setInterval(doScrollLeft, 6);
			});	
			
			// mouseout left hot spot
			$mom.find(options.scrollingHotSpotLeft).bind('mouseout',function(){
				clearInterval($mom.leftScrollInterval);
				scrollXpos = 0;
			});
			
			// scrolling speed booster left
			$mom.find(options.scrollingHotSpotLeft).bind('mousedown',function(){
				booster = options.mouseDownSpeedBooster;
			});
			
			// The function that does the actual scrolling left
			var doScrollLeft = function()
			{	
				if(scrollXpos > 0) {
					$mom.find(options.scrollWrapper).scrollLeft($mom.find(options.scrollWrapper).scrollLeft() - (scrollXpos*booster));
				}
				showHideHotSpots();
			};
			
			// **************************************************
			// Hot spot functions
			// **************************************************
			
			// Function for showing and hiding hot spots depending on the
			// offset of the scrolling
			function showHideHotSpots()
			{
				// When you can't scroll further left
				// the left scroll hot spot should be hidden
				// and the right hot spot visible
				if($mom.find(options.scrollWrapper).scrollLeft() === 0)
				{
					hideLeftHotSpot();
					showRightHotSpot();
				}
				// When you can't scroll further right
				// the right scroll hot spot should be hidden
				// and the left hot spot visible
				else if(($mom.scrollableAreaWidth) <= ($mom.find(options.scrollWrapper).innerWidth() + $mom.find(options.scrollWrapper).scrollLeft()))
				{
					hideRightHotSpot();
					showLeftHotSpot();
				}
				// If you are somewhere in the middle of your
				// scrolling, both hot spots should be visible
				else
				{
					showRightHotSpot();
					showLeftHotSpot();
				}

			}
			
			// Function for making the hot spot background visible
			function makeHotSpotBackgroundsVisible()
			{
				// Alter the CSS (SmoothDivScroll.css) if you want to customize
				// the look'n'feel of the visible hot spots
				
				// The left hot spot
				$mom.find(options.scrollingHotSpotLeft).addClass("scrollingHotSpotLeftVisible");

				// The right hot spot
				$mom.find(options.scrollingHotSpotRight).addClass("scrollingHotSpotRightVisible");
			}
			
			// Hide the hot spot backgrounds.
			function hideHotSpotBackgrounds()
			{
				clearInterval($mom.hideHotSpotBackgroundsInterval);
				
				// Fade out the left hot spot
				$mom.find(options.scrollingHotSpotLeft).fadeTo("slow", 0.0, function(){
					$mom.find(options.scrollingHotSpotLeft).removeClass("scrollingHotSpotLeftVisible");
				});

				// Fade out the right hot spot
				$mom.find(options.scrollingHotSpotRight).fadeTo("slow", 0.0, function(){
					$mom.find(options.scrollingHotSpotRight).removeClass("scrollingHotSpotRightVisible");
				});
			}
			
	});
};

})(jQuery);

